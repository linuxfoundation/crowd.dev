import { proxyActivities } from '@temporalio/workflow'

import type * as depsDevActivities from '../activities'
import { buildDependentCountsSql } from '../queries/dependentCountsSql'

const { bqExportToGcs } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '1 hour',
  retry: { maximumAttempts: 3, initialInterval: '1 minute', backoffCoefficient: 2 },
})

const { gcsParquetToStaging } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '2 hours',
  heartbeatTimeout: '2 minutes',
  retry: { maximumAttempts: 2 },
})

const { mergeStagingToTable } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '1 hour',
  retry: { maximumAttempts: 1 },
})

const STAGING_TABLE = 'staging.osspckgs_dependent_counts_raw'

const STAGING_DDL = `
CREATE UNLOGGED TABLE IF NOT EXISTS staging.osspckgs_dependent_counts_raw (
  purl                      text,
  dependent_packages_count  bigint,
  dependent_repos_count     bigint
)
`

// Strip @version from both sides — BQ ANY_VALUE(Purl) is non-deterministic across separate
// query executions and may include or omit the version suffix.
const MERGE_SQL = `
UPDATE packages SET
  dependent_packages_count = s.dependent_packages_count,
  dependent_repos_count    = s.dependent_repos_count,
  last_synced_at           = NOW()
FROM staging.osspckgs_dependent_counts_raw s
WHERE REGEXP_REPLACE(packages.purl, '@[^@]+$', '') = REGEXP_REPLACE(s.purl, '@[^@]+$', '')
`

const PG_COLUMNS = ['purl', 'dependent_packages_count', 'dependent_repos_count']

export async function ingestDependentCounts(opts: {
  runId: string
  snapshotDate: string
  reuseExports?: boolean
  exportName?: string
}): Promise<void> {
  const exportResult = await bqExportToGcs({
    jobKind: 'dependent_counts',
    sql: buildDependentCountsSql(opts.snapshotDate),
    runId: opts.runId,
    syncMode: 'full',
    snapshotAt: opts.snapshotDate,
    maxBytesGb: 2000,
    reuseExports: opts.reuseExports,
    exportName: opts.exportName,
  })

  await gcsParquetToStaging({
    jobId: exportResult.jobId,
    gcsPrefix: exportResult.gcsPrefix,
    stagingTable: STAGING_TABLE,
    stagingDdl: STAGING_DDL,
    pgColumns: PG_COLUMNS,
  })

  await mergeStagingToTable({
    jobId: exportResult.jobId,
    mergeSql: MERGE_SQL,
    tableNames: 'packages',
  })
}
