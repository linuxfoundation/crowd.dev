import { proxyActivities } from '@temporalio/workflow'

import type * as depsDevActivities from '../activities'
import { toSystemsFilter } from '../queries/systems'
import { buildVersionsFullSql, buildVersionsIncrementalSql } from '../queries/versionsSql'

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

const { dropVersionsIndexes, rebuildVersionsIndexes } = proxyActivities<typeof depsDevActivities>({
  // Index builds on large partitioned tables can take hours.
  startToCloseTimeout: '6 hours',
  retry: { maximumAttempts: 2, initialInterval: '1 minute' },
})

const { dropVersionsConstraints, rebuildVersionsConstraints } = proxyActivities<
  typeof depsDevActivities
>({
  // FK validation on large partitioned tables can take hours.
  startToCloseTimeout: '6 hours',
  retry: { maximumAttempts: 2, initialInterval: '1 minute' },
})

const STAGING_TABLE = 'staging.osspckgs_versions_raw'

const STAGING_DDL = `
CREATE UNLOGGED TABLE IF NOT EXISTS staging.osspckgs_versions_raw (
  ecosystem     text,
  raw_name      text,
  purl          text,
  number        text,
  published_at  timestamptz,
  is_prerelease bool,
  licenses      text[]
)
`

const MERGE_SQL = `
INSERT INTO versions (
  package_id, ecosystem, namespace, name, number, published_at, is_prerelease, licenses, last_synced_at,
  created_at
)
SELECT
  p.id, s.ecosystem, p.namespace, p.name, s.number, s.published_at, s.is_prerelease, s.licenses, NOW(),
  NOW()
FROM staging.osspckgs_versions_raw s
JOIN packages p ON p.purl = s.purl
ON CONFLICT (package_id, number) DO NOTHING
`

// Full-load variant: UNIQUE constraint is dropped before the chunk loop so plain INSERT is safe.
// DISTINCT ON (p.id, s.number) deduplicates before INSERT — guards against duplicate (purl, number)
// rows in BQ data that would cause the UNIQUE constraint rebuild to fail.
const MERGE_SQL_FULL = `
INSERT INTO versions (
  package_id, ecosystem, namespace, name, number, published_at, is_prerelease, licenses, last_synced_at,
  created_at
)
SELECT DISTINCT ON (p.id, s.number)
  p.id, s.ecosystem, p.namespace, p.name, s.number, s.published_at, s.is_prerelease, s.licenses, NOW(),
  NOW()
FROM staging.osspckgs_versions_raw s
JOIN packages p ON p.purl = s.purl
ORDER BY p.id, s.number, s.published_at DESC NULLS LAST
`

// SET LOCAL scopes settings to this transaction only.
// synchronous_commit=off skips WAL flush wait — safe for plain INSERT on full loads.
// max_parallel_workers_per_gather parallelises the SELECT side of INSERT...SELECT.
// session_replication_role=replica would skip FK trigger checks but requires superuser —
// blocked on Oracle Cloud managed PostgreSQL regardless of REPLICATION role.
const MERGE_PREPARE_SQL = [
  `SET LOCAL work_mem = '512MB'`,
  `SET LOCAL synchronous_commit = off`,
  `SET LOCAL max_parallel_workers_per_gather = 8`,
]

const PG_COLUMNS = [
  'ecosystem',
  'raw_name',
  'purl',
  'number',
  'published_at',
  'is_prerelease',
  'licenses',
]

const ROWS_PER_CHUNK = 1_000_000

export async function ingestVersions(opts: {
  runId: string
  syncMode: 'full' | 'incremental'
  today: string
  watermark: string | null
  ecosystems?: string[]
  reuseExports?: boolean
  exportName?: string
}): Promise<{ rowCountBq: number }> {
  const systems = toSystemsFilter(opts.ecosystems)
  const sql =
    opts.syncMode === 'full'
      ? buildVersionsFullSql(systems)
      : buildVersionsIncrementalSql(opts.today, opts.watermark ?? '', systems)

  const exportResult = await bqExportToGcs({
    jobKind: 'versions',
    sql,
    runId: opts.runId,
    syncMode: opts.syncMode,
    snapshotAt: opts.today,
    maxBytesGb: 400,
    reuseExports: opts.reuseExports,
    exportName: opts.exportName,
  })

  const { fileNames, rowCounts } = await listParquetFiles({ gcsPrefix: exportResult.gcsPrefix })
  const totalFiles = fileNames.length

  if (totalFiles === 0) {
    await mergeStagingToTable({
      jobId: exportResult.jobId,
      mergeSql: [],
      tableNames: [],
      isFinal: true,
    })
    return { rowCountBq: exportResult.rowCount }
  }

  if (opts.syncMode === 'full') {
    await dropVersionsConstraints()
    await dropVersionsIndexes()
  }

  try {
    const totalRows = rowCounts.reduce((a, b) => a + b, 0)
    const filesPerChunk =
      totalRows > 0
        ? Math.max(1, Math.round((ROWS_PER_CHUNK * fileNames.length) / totalRows))
        : Math.min(fileNames.length, 2)
    const totalChunks = Math.ceil(fileNames.length / filesPerChunk)
    let priorRowsAffected = 0
    let priorStagingRows = 0
    const priorTableRowCounts: Record<string, number> = {}

    for (let chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++) {
      const start = chunkIndex * filesPerChunk
      const chunk = fileNames.slice(start, start + filesPerChunk)
      const isFinal = chunkIndex === totalChunks - 1

      const { rowsLoaded } = await gcsParquetToStaging({
        jobId: exportResult.jobId,
        stagingTable: STAGING_TABLE,
        stagingDdl: STAGING_DDL,
        pgColumns: PG_COLUMNS,
        timestampColumns: ['published_at'],
        fileNames: chunk,
        filesOffset: start,
        totalFiles,
        priorStagingRows,
      })
      priorStagingRows += rowsLoaded

      const { rowsAffected, tableRowCounts } = await mergeStagingToTable({
        jobId: exportResult.jobId,
        prepareSql: MERGE_PREPARE_SQL,
        mergeSql: opts.syncMode === 'full' ? MERGE_SQL_FULL : MERGE_SQL,
        tableNames: 'versions',
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

    if (opts.syncMode === 'full') {
      await rebuildVersionsIndexes()
      await rebuildVersionsConstraints()
    }
  } catch (err) {
    if (opts.syncMode === 'full') {
      try {
        await rebuildVersionsIndexes()
      } catch (_) {
        /* best-effort */
      }
      try {
        await rebuildVersionsConstraints()
      } catch (_) {
        /* best-effort */
      }
    }
    throw err
  }
  return { rowCountBq: exportResult.rowCount }
}
