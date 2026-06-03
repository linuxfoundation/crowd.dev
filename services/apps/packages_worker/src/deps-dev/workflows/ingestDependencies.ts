import { proxyActivities } from '@temporalio/workflow'

import type * as depsDevActivities from '../activities'
import { buildDepsFullSql, buildDepsIncrementalSql } from '../queries/depsSql'
import { toSystemsFilter } from '../queries/systems'

const { bqExportToGcs } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '2 hours',
  retry: { maximumAttempts: 3, initialInterval: '1 minute', backoffCoefficient: 2 },
})

const { listParquetFiles } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '5 minutes',
  retry: { maximumAttempts: 3 },
})

const { gcsParquetToStaging } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '4 hours',
  heartbeatTimeout: '2 minutes',
  retry: { maximumAttempts: 2 },
})

const { mergeStagingToTable } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '2 hours',
  retry: { maximumAttempts: 1 },
})

const { createVersionsLookup } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '2 hours',
  retry: { maximumAttempts: 2, initialInterval: '30 seconds' },
})

const { dropPackageDepsIndexes, rebuildPackageDepsIndexes } = proxyActivities<
  typeof depsDevActivities
>({
  // Index builds on 1B+ rows can take hours — long timeout required.
  startToCloseTimeout: '12 hours',
  retry: { maximumAttempts: 2, initialInterval: '1 minute' },
})

const { dropPackageDepsConstraints, rebuildPackageDepsConstraints } = proxyActivities<
  typeof depsDevActivities
>({
  // FK validation on 1B+ rows can take hours.
  startToCloseTimeout: '12 hours',
  retry: { maximumAttempts: 2, initialInterval: '1 minute' },
})

const STAGING_TABLE = 'staging.osspckgs_deps_raw'

const STAGING_DDL = `
CREATE UNLOGGED TABLE IF NOT EXISTS staging.osspckgs_deps_raw (
  ecosystem          text,
  root_name          text,
  root_version       text,
  to_name            text,
  to_version         text,
  version_constraint text
)
`

// staging.osspckgs_versions_lookup is built once before the chunk loop by createVersionsLookup.
// Using a temp table per chunk would rebuild 4GB for every chunk on npm (150+ chunks × rebuild).
// The lookup table is unpartitioned — with ANALYZE the planner chooses hash join instead of the
// 32-partition fan-out that hits the partitioned versions table when joining on
// (ecosystem, namespace, name, number) rather than the partition key (package_id).
//
// Dep version LEFT JOIN keeps the original versions table — that join uses (package_id, number)
// which matches the partition key, so PG routes each row to exactly one partition (fast).
const MERGE_SQL = `
INSERT INTO package_dependencies (
  package_id, version_id, depends_on_id, depends_on_version_id,
  version_constraint, dependency_kind, is_optional
)
SELECT
  pv.package_id, pv.id, pd.id, dv.id,
  sp.version_constraint, 'direct', FALSE
FROM staging.osspckgs_deps_raw sp
JOIN staging.osspckgs_versions_lookup pv ON pv.ecosystem = sp.ecosystem
  AND pv.ns = CASE
      WHEN sp.ecosystem = 'maven'   THEN SPLIT_PART(sp.root_name, ':', 1)
      WHEN sp.root_name LIKE '@%/%' THEN SPLIT_PART(sp.root_name, '/', 1)
      ELSE '' END
  AND pv.name = CASE
      WHEN sp.ecosystem = 'maven'   THEN SPLIT_PART(sp.root_name, ':', 2)
      WHEN sp.root_name LIKE '@%/%' THEN SPLIT_PART(sp.root_name, '/', 2)
      ELSE sp.root_name END
  AND pv.number = sp.root_version
JOIN packages pd ON pd.ecosystem = sp.ecosystem
  AND COALESCE(pd.namespace, '') = CASE
      WHEN sp.ecosystem = 'maven'  THEN SPLIT_PART(sp.to_name, ':', 1)
      WHEN sp.to_name LIKE '@%/%'  THEN SPLIT_PART(sp.to_name, '/', 1)
      ELSE '' END
  AND pd.name = CASE
      WHEN sp.ecosystem = 'maven'  THEN SPLIT_PART(sp.to_name, ':', 2)
      WHEN sp.to_name LIKE '@%/%'  THEN SPLIT_PART(sp.to_name, '/', 2)
      ELSE sp.to_name END
LEFT JOIN versions dv ON dv.package_id = pd.id AND dv.number = sp.to_version
ON CONFLICT (version_id, depends_on_id, dependency_kind) DO NOTHING
`

// Full-load variant: UNIQUE constraint is dropped before the chunk loop so plain INSERT is safe.
// DISTINCT ON (pv.id, pd.id) deduplicates before INSERT — BQ can emit multiple rows for the same
// (root_name, root_version, to_name) with different to_version values; both resolve to the same
// (version_id, depends_on_id) pair, which violates the UNIQUE constraint on rebuild.
const MERGE_SQL_FULL = `
INSERT INTO package_dependencies (
  package_id, version_id, depends_on_id, depends_on_version_id,
  version_constraint, dependency_kind, is_optional
)
SELECT DISTINCT ON (pv.id, pd.id)
  pv.package_id, pv.id, pd.id, dv.id,
  sp.version_constraint, 'direct', FALSE
FROM staging.osspckgs_deps_raw sp
JOIN staging.osspckgs_versions_lookup pv ON pv.ecosystem = sp.ecosystem
  AND pv.ns = CASE
      WHEN sp.ecosystem = 'maven'   THEN SPLIT_PART(sp.root_name, ':', 1)
      WHEN sp.root_name LIKE '@%/%' THEN SPLIT_PART(sp.root_name, '/', 1)
      ELSE '' END
  AND pv.name = CASE
      WHEN sp.ecosystem = 'maven'   THEN SPLIT_PART(sp.root_name, ':', 2)
      WHEN sp.root_name LIKE '@%/%' THEN SPLIT_PART(sp.root_name, '/', 2)
      ELSE sp.root_name END
  AND pv.number = sp.root_version
JOIN packages pd ON pd.ecosystem = sp.ecosystem
  AND COALESCE(pd.namespace, '') = CASE
      WHEN sp.ecosystem = 'maven'  THEN SPLIT_PART(sp.to_name, ':', 1)
      WHEN sp.to_name LIKE '@%/%'  THEN SPLIT_PART(sp.to_name, '/', 1)
      ELSE '' END
  AND pd.name = CASE
      WHEN sp.ecosystem = 'maven'  THEN SPLIT_PART(sp.to_name, ':', 2)
      WHEN sp.to_name LIKE '@%/%'  THEN SPLIT_PART(sp.to_name, '/', 2)
      ELSE sp.to_name END
LEFT JOIN versions dv ON dv.package_id = pd.id AND dv.number = sp.to_version
ORDER BY pv.id, pd.id, sp.to_version DESC NULLS LAST
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
  'root_name',
  'root_version',
  'to_name',
  'to_version',
  'version_constraint',
]

const ROWS_PER_CHUNK = 1_000_000

export async function ingestDependencies(opts: {
  runId: string
  syncMode: 'full' | 'incremental'
  today: string
  watermark: string | null
  ecosystems?: string[]
  reuseExports?: boolean
  depsTableOption?: 'A' | 'B'
  exportName?: string
}): Promise<void> {
  const systems = toSystemsFilter(opts.ecosystems)
  const tableOption = opts.depsTableOption ?? 'A'
  const sql =
    opts.syncMode === 'full'
      ? buildDepsFullSql(systems, tableOption)
      : buildDepsIncrementalSql(opts.today, opts.watermark ?? '', systems, tableOption)

  const exportResult = await bqExportToGcs({
    jobKind: 'package_dependencies',
    sql,
    runId: opts.runId,
    syncMode: opts.syncMode,
    snapshotAt: opts.today,
    maxBytesGb: 3000,
    reuseExports: opts.reuseExports,
    exportName: opts.exportName,
  })

  const { fileNames, rowCounts } = await listParquetFiles({ gcsPrefix: exportResult.gcsPrefix })
  const totalFiles = fileNames.length
  const totalRows = rowCounts.reduce((a, b) => a + b, 0)

  if (totalFiles === 0 || totalRows === 0) {
    await mergeStagingToTable({
      jobId: exportResult.jobId,
      mergeSql: [],
      tableNames: [],
      isFinal: true,
    })
    return
  }

  await createVersionsLookup({ ecosystems: opts.ecosystems })

  if (opts.syncMode === 'full') {
    await dropPackageDepsConstraints()
    await dropPackageDepsIndexes()
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
        tableNames: 'package_dependencies',
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
      await rebuildPackageDepsIndexes()
      await rebuildPackageDepsConstraints()
    }
  } catch (err) {
    if (opts.syncMode === 'full') {
      try {
        await rebuildPackageDepsIndexes()
      } catch (_) {
        /* best-effort */
      }
      try {
        await rebuildPackageDepsConstraints()
      } catch (_) {
        /* best-effort */
      }
    }
    throw err
  }
}
