import { proxyActivities } from '@temporalio/workflow'

import type * as depsDevActivities from '../activities'
import { buildDependentCountsSql } from '../queries/dependentCountsSql'

const { bqExportToGcs } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '1 hour',
  retry: { maximumAttempts: 3, initialInterval: '1 minute', backoffCoefficient: 2 },
})

const { listParquetFiles } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '5 minutes',
  retry: { maximumAttempts: 3 },
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
WHERE packages.purl = REGEXP_REPLACE(s.purl, '@[^@]+$', '')
`

const PG_COLUMNS = ['purl', 'dependent_packages_count', 'dependent_repos_count']

const ROWS_PER_CHUNK = 1_000_000

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

  const { fileNames, rowCounts } = await listParquetFiles({ gcsPrefix: exportResult.gcsPrefix })
  const totalFiles = fileNames.length

  if (totalFiles === 0) {
    await mergeStagingToTable({ jobId: exportResult.jobId, mergeSql: [], tableNames: [], isFinal: true })
    return
  }

  const totalRows = rowCounts.reduce((a, b) => a + b, 0)
  const filesPerChunk = totalRows > 0
    ? Math.max(1, Math.round((ROWS_PER_CHUNK * fileNames.length) / totalRows))
    : Math.min(fileNames.length, 2)
  const totalChunks = Math.ceil(fileNames.length / filesPerChunk)
  let priorRowsAffected = 0
  let priorStagingRows = 0
  let priorTableRowCounts: Record<string, number> = {}

  for (let chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++) {
    const start = chunkIndex * filesPerChunk
    const chunk = fileNames.slice(start, start + filesPerChunk)
    const isFinal = chunkIndex === totalChunks - 1

    const { rowsLoaded } = await gcsParquetToStaging({
      jobId: exportResult.jobId,
      stagingTable: STAGING_TABLE,
      stagingDdl: STAGING_DDL,
      pgColumns: PG_COLUMNS,
      fileNames: chunk,
      filesOffset: start,
      totalFiles,
      priorStagingRows,
    })
    priorStagingRows += rowsLoaded

    const { rowsAffected, tableRowCounts } = await mergeStagingToTable({
      jobId: exportResult.jobId,
      mergeSql: MERGE_SQL,
      tableNames: 'packages',
      isFinal,
      priorRowsAffected,
      priorTableRowCounts,
      chunkInfo: { index: chunkIndex, total: totalChunks },
    })

    priorRowsAffected += rowsAffected
    if (!isFinal) {
      for (const [k, v] of Object.entries(tableRowCounts)) {
        priorTableRowCounts[k] = (priorTableRowCounts[k] ?? 0) + v
      }
    }
  }
}
