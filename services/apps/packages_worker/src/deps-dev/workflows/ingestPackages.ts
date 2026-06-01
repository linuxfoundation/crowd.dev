import { proxyActivities } from '@temporalio/workflow'

import type * as depsDevActivities from '../activities'
import { buildPackagesFullSql, buildPackagesIncrementalSql } from '../queries/packagesSql'
import { toSystemsFilter } from '../queries/systems'

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

const STAGING_TABLE = 'staging.osspckgs_packages_raw'

const STAGING_DDL = `
CREATE UNLOGGED TABLE IF NOT EXISTS staging.osspckgs_packages_raw (
  ecosystem        text,
  raw_name         text,
  purl             text,
  description      text,
  licenses         text[],
  latest_version   text,
  declared_repo_url text,
  homepage         text,
  first_release_at timestamptz,
  latest_release_at timestamptz,
  versions_count   int
)
`

const MERGE_SQL = `
INSERT INTO packages (
  ecosystem, namespace, name, purl, description, licenses,
  latest_version, declared_repository_url, homepage,
  first_release_at, latest_release_at, versions_count,
  ingestion_source, last_synced_at
)
SELECT
  s.ecosystem,
  CASE
    WHEN s.ecosystem = 'maven' THEN SPLIT_PART(s.raw_name, ':', 1)
    WHEN s.raw_name LIKE '@%/%'  THEN SPLIT_PART(s.raw_name, '/', 1)
    ELSE NULL
  END,
  CASE
    WHEN s.ecosystem = 'maven' THEN SPLIT_PART(s.raw_name, ':', 2)
    WHEN s.raw_name LIKE '@%/%'  THEN SPLIT_PART(s.raw_name, '/', 2)
    ELSE s.raw_name
  END,
  s.purl, s.description, s.licenses,
  s.latest_version, s.declared_repo_url, s.homepage,
  s.first_release_at, s.latest_release_at, s.versions_count,
  'deps_dev', NOW()
FROM staging.osspckgs_packages_raw s
ON CONFLICT (purl) DO NOTHING
`

const PG_COLUMNS = [
  'ecosystem', 'raw_name', 'purl', 'description', 'licenses',
  'latest_version', 'declared_repo_url', 'homepage',
  'first_release_at', 'latest_release_at', 'versions_count',
]

const ROWS_PER_CHUNK = 1_000_000

export async function ingestPackages(opts: {
  runId: string
  syncMode: 'full' | 'incremental'
  today: string
  watermark: string | null
  ecosystems?: string[]
  reuseExports?: boolean
  exportName?: string
}): Promise<void> {
  const systems = toSystemsFilter(opts.ecosystems)
  const sql =
    opts.syncMode === 'full'
      ? buildPackagesFullSql(systems)
      : buildPackagesIncrementalSql(opts.today, opts.watermark!, systems)

  const exportResult = await bqExportToGcs({
    jobKind: 'packages',
    sql,
    runId: opts.runId,
    syncMode: opts.syncMode,
    snapshotAt: opts.today,
    maxBytesGb: opts.syncMode === 'full' ? 6000 : 400,
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

  for (let chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++) {
    const start = chunkIndex * filesPerChunk
    const chunk = fileNames.slice(start, start + filesPerChunk)
    const isFinal = chunkIndex === totalChunks - 1

    const { rowsLoaded } = await gcsParquetToStaging({
      jobId: exportResult.jobId,
      stagingTable: STAGING_TABLE,
      stagingDdl: STAGING_DDL,
      pgColumns: PG_COLUMNS,
      timestampColumns: ['first_release_at', 'latest_release_at'],
      fileNames: chunk,
      filesOffset: start,
      totalFiles,
      priorStagingRows,
    })
    priorStagingRows += rowsLoaded

    const { rowsAffected } = await mergeStagingToTable({
      jobId: exportResult.jobId,
      mergeSql: MERGE_SQL,
      tableNames: 'packages',
      isFinal,
      priorRowsAffected,
      chunkInfo: { index: chunkIndex, total: totalChunks },
    })

    priorRowsAffected += rowsAffected
  }
}
