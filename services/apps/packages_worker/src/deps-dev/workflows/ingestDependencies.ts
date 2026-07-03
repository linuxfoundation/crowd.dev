import { ApplicationFailure, proxyActivities } from '@temporalio/workflow'

import type * as depsDevActivities from '../activities'
import {
  DEPS_DEFAULT_ECOSYSTEMS,
  buildDepsFullSql,
  buildDepsIncrementalSql,
} from '../queries/depsSql'

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
  // A single ~1M-row chunk merge into the live-index/live-FK package_dependencies table (incremental
  // does not drop indexes) can run long; 4h gives headroom over the previously-observed 2h overrun.
  startToCloseTimeout: '4 hours',
  retry: { maximumAttempts: 1 },
})

const { getResumeExport } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 3 },
})

const { createVersionsLookup } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '2 hours',
  retry: { maximumAttempts: 2, initialInterval: '30 seconds' },
})

const { dropPackageDepsIndexes, rebuildPackageDepsIndexes } = proxyActivities<
  typeof depsDevActivities
>({
  // Index builds + parallel dedup on 1B+ rows — 24h covers worst-case sequential retry.
  startToCloseTimeout: '24 hours',
  retry: { maximumAttempts: 2, initialInterval: '1 minute' },
})

const { dropPackageDepsConstraints, rebuildPackageDepsConstraints } = proxyActivities<
  typeof depsDevActivities
>({
  // FK validation on 1B+ rows can take hours.
  startToCloseTimeout: '24 hours',
  retry: { maximumAttempts: 2, initialInterval: '1 minute' },
})

const { setJobStep } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '30 seconds',
  retry: { maximumAttempts: 3 },
})

const { checkEdgeSnapshotQuality } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '5 minutes',
  retry: { maximumAttempts: 3 },
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
  version_constraint, dependency_kind, is_optional, created_at, updated_at
)
SELECT
  pv.package_id, pv.id, pd.id, dv.id,
  sp.version_constraint, 'direct', FALSE, NOW(), NOW()
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
  version_constraint, dependency_kind, is_optional, created_at, updated_at
)
SELECT DISTINCT ON (pv.id, pd.id)
  pv.package_id, pv.id, pd.id, dv.id,
  sp.version_constraint, 'direct', FALSE, NOW(), NOW()
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

// Fill-constraints variant: UNIQUE constraint stays in place (not dropped), so ON CONFLICT is valid.
// Upserts version_constraint only for rows where it is currently NULL — safe to run against a table
// already populated by --deps-table-b (which sets version_constraint = NULL for all rows).
// DISTINCT ON matches MERGE_SQL_FULL to resolve duplicate (root, dep) pairs from BQ before the upsert.
const MERGE_SQL_FILL_CONSTRAINTS = `
INSERT INTO package_dependencies (
  package_id, version_id, depends_on_id, depends_on_version_id,
  version_constraint, dependency_kind, is_optional, created_at, updated_at
)
SELECT DISTINCT ON (pv.id, pd.id)
  pv.package_id, pv.id, pd.id, dv.id,
  sp.version_constraint, 'direct', FALSE, NOW(), NOW()
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
ON CONFLICT (version_id, depends_on_id, dependency_kind) DO UPDATE
  SET version_constraint = EXCLUDED.version_constraint
  WHERE package_dependencies.version_constraint IS NULL
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
  fillConstraints?: boolean // re-export full BQ data, upsert version_constraint only where NULL
  // Resume a partially-merged prior job: skip the BQ export, reuse that job's exact parquet files,
  // and restart the chunk loop where its staging load left off. Idempotent modes only (see gate).
  resumeJobId?: number
}): Promise<{ rowCountBq: number }> {
  const ecosystems = opts.ecosystems ?? DEPS_DEFAULT_ECOSYSTEMS
  const isFill = opts.fillConstraints === true
  // Fill mode forces Option A — Option B selects NULL for version_constraint, making the fill a no-op.
  const tableOption = isFill ? 'A' : (opts.depsTableOption ?? 'A')
  // Full/fill scan the *Latest views; incremental reads the `today` partition. Drives both the SQL
  // source below and which source the guard probes — they must match.
  const fullScan = opts.syncMode === 'full' || isFill

  const resume = opts.resumeJobId != null

  // Resume is only safe for idempotent merges: incremental uses ON CONFLICT DO NOTHING and fill uses
  // ON CONFLICT DO UPDATE, so re-running an overlapping chunk no-ops. A full (non-fill) load uses a
  // plain INSERT with the UNIQUE constraint dropped, so a re-merged chunk would duplicate rows.
  if (resume && fullScan && !isFill) {
    throw ApplicationFailure.nonRetryable(
      `resume (resumeJobId) is not supported for full loads — plain INSERT would duplicate rows. ` +
        `Use incremental or --fill-constraints.`,
      'RESUME_UNSUPPORTED',
    )
  }

  let exportResult: { jobId: number; gcsPrefix: string; rowCount: number }
  // Files already loaded into staging by the prior (resumed) job, and rows it had already merged.
  let resumeProgressDone = 0
  let resumeRowCountPg = 0
  if (opts.resumeJobId != null) {
    // Reuse the prior job's exact export by id — bypasses reuse-by-kind (which excludes 'failed'
    // jobs and could grab a different, older export) and skips the multi-hour BQ scan entirely.
    // Validate hard here (non-retryable) so a bad id fails immediately instead of corrupting state.
    const prior = await getResumeExport({ jobId: opts.resumeJobId })
    if (!prior) {
      throw ApplicationFailure.nonRetryable(
        `resume job ${opts.resumeJobId} not found`,
        'RESUME_INVALID',
      )
    }
    if (prior.jobKind !== 'package_dependencies') {
      throw ApplicationFailure.nonRetryable(
        `resume job ${opts.resumeJobId} is kind '${prior.jobKind}', expected 'package_dependencies'`,
        'RESUME_INVALID',
      )
    }
    // Resumable = has an export to reuse with unfinished merge work: exported (no chunk merged yet),
    // loading/merging (interrupted mid-run), failed (the common case). done = nothing to do;
    // cleaned = parquet already deleted; pending/exporting = no export produced yet.
    const RESUMABLE_STATUSES = ['exported', 'loading', 'merging', 'failed']
    if (!RESUMABLE_STATUSES.includes(prior.status)) {
      throw ApplicationFailure.nonRetryable(
        `resume job ${opts.resumeJobId} has status '${prior.status}' — not resumable ` +
          `(expected one of: ${RESUMABLE_STATUSES.join(', ')})`,
        'RESUME_INVALID',
      )
    }
    if (!prior.gcsPrefix) {
      throw ApplicationFailure.nonRetryable(
        `resume job ${opts.resumeJobId} has no gcs_prefix — nothing was exported to resume from`,
        'RESUME_INVALID',
      )
    }
    exportResult = { jobId: prior.jobId, gcsPrefix: prior.gcsPrefix, rowCount: 0 }
    resumeProgressDone = prior.progressDone
    resumeRowCountPg = prior.rowCountPg
  } else {
    // Guard against corrupt deps.dev resolved-graph snapshots BEFORE the (multi-hour) export.
    // Skip when reusing a prior export — we're re-importing already-validated parquet, not
    // scanning the live snapshot. Both full (*Latest = newest snapshot) and incremental can hit
    // a bad snapshot, so the guard runs for both. Probes only resolved-graph ecosystems; a clean
    // GO/NUGET-only run finds no canaries and passes through.
    //
    // Option A only: the guard's canary ratios are DependencyGraphEdges-schema-specific. Option B
    // ingests the separate Dependencies/DependenciesLatest table, which the 2026-06 corruption was
    // never observed in and has no calibrated baseline — probing Edges there would "validate" a table
    // we don't ingest (false confidence) and could abort a healthy Option B run when only Edges is bad.
    // Option B is a manual, non-scheduled cost-experiment path (--deps-table-b); leave it unguarded by
    // design rather than invent a guard for an unproven threat. Option A is the production default.
    if (!opts.reuseExports && tableOption === 'A') {
      const guard = await checkEdgeSnapshotQuality({
        snapshotDate: opts.today,
        ecosystems,
        fullScan,
      })
      if (!guard.ok) {
        throw ApplicationFailure.nonRetryable(
          `edge snapshot quality guard failed for ${opts.today}: ${guard.reason}. ` +
            `Slack alert sent. Aborting before export to preserve existing package_dependencies and compute.`,
          'EDGE_SNAPSHOT_GUARD',
        )
      }
    }
    // Fill mode always uses full SQL — needs all rows to find which have NULL version_constraint in DB.
    const sql = fullScan
      ? buildDepsFullSql(ecosystems, tableOption)
      : buildDepsIncrementalSql(opts.today, opts.watermark ?? '', ecosystems, tableOption)

    exportResult = await bqExportToGcs({
      jobKind: 'package_dependencies',
      sql,
      runId: opts.runId,
      // Report the PHYSICAL scan mode, not the requested one. A fill run (fillConstraints) forces a
      // full *Latest scan even when opts.syncMode is 'incremental'; passing the raw mode would record
      // the job as incremental and make bqExportToGcs pick the INCREMENTAL byte-ceiling env override
      // for a query that's actually full. fullScan already gates the SQL + maxBytesGb below.
      syncMode: fullScan ? 'full' : opts.syncMode,
      snapshotAt: opts.today,
      // Full/fill scan the *Latest views (everything) → 25000. Incremental is a snapshot edge-diff
      // (today vs watermark partitions of DependencyGraphEdges + GoRequirements + NuGetRequirements);
      // measured ~4.1TB for Option A. 10000 leaves ~2.4x headroom and still trips a runaway full-table
      // scan. Overridable via BQ_DATASET_INGEST_PACKAGE_DEPENDENCIES[_INCREMENTAL]_MAX_BQ_GB (see README).
      maxBytesGb: fullScan ? 25000 : 10000,
      reuseExports: opts.reuseExports,
      exportName: opts.exportName,
      ecosystems,
    })
  }

  const { fileNames, rowCounts } = await listParquetFiles({ gcsPrefix: exportResult.gcsPrefix })
  const totalFiles = fileNames.length
  const totalRows = rowCounts.reduce((a, b) => a + b, 0)

  // Resume must find the exact parquet it recorded. Empty means GCS expired/deleted the files —
  // fail loudly rather than fall through to the 0-row early-return below, which would mark the
  // job 'done' and silently abandon the un-merged tail.
  if (opts.resumeJobId != null && (totalFiles === 0 || totalRows === 0)) {
    throw ApplicationFailure.nonRetryable(
      `resume job ${opts.resumeJobId}: no parquet files at ${exportResult.gcsPrefix} ` +
        `(export expired or deleted) — run a fresh export instead of --resume-job`,
      'RESUME_INVALID',
    )
  }

  if (totalFiles === 0 || totalRows === 0) {
    await mergeStagingToTable({
      jobId: exportResult.jobId,
      mergeSql: [],
      tableNames: [],
      isFinal: true,
    })
    return { rowCountBq: exportResult.rowCount }
  }

  await setJobStep({ jobId: exportResult.jobId, step: 'creating_lookup' })
  await createVersionsLookup({ ecosystems })

  if (opts.syncMode === 'full' && !isFill) {
    await setJobStep({ jobId: exportResult.jobId, step: 'drop_constraints' })
    await dropPackageDepsConstraints()
    await setJobStep({ jobId: exportResult.jobId, step: 'drop_indexes' })
    await dropPackageDepsIndexes()
  }

  try {
    const filesPerChunk =
      totalRows > 0
        ? Math.max(1, Math.round((ROWS_PER_CHUNK * fileNames.length) / totalRows))
        : Math.min(fileNames.length, 2)
    const totalChunks = Math.ceil(fileNames.length / filesPerChunk)
    // Resume: the prior job recorded files loaded (progress:done) but not a per-merge-chunk index,
    // and the merge lags the load by up to one chunk. Restart one chunk before where the load
    // stopped and let ON CONFLICT no-op the overlap. Clamp to [0, totalChunks). Seed priorRowsAffected
    // with the prior job's merged count so the final 'done' row_count_pg stays roughly accurate.
    const startChunk = resume
      ? Math.max(0, Math.min(Math.floor(resumeProgressDone / filesPerChunk) - 1, totalChunks - 1))
      : 0
    let priorRowsAffected = resume ? resumeRowCountPg : 0
    let priorStagingRows = 0
    const priorTableRowCounts: Record<string, number> = {}

    // Advance the phase label to 'merging' before the loop. The merge itself sets status='merging'
    // but never a step, so without this the monitor keeps showing the last phase (creating_lookup for
    // fill/incremental, drop_indexes for full) alongside the 'merging' status. Set once, not per chunk.
    await setJobStep({ jobId: exportResult.jobId, step: 'merging' })

    for (let chunkIndex = startChunk; chunkIndex < totalChunks; chunkIndex++) {
      const start = chunkIndex * filesPerChunk
      const chunk = fileNames.slice(start, start + filesPerChunk)
      const isLastChunk = chunkIndex === totalChunks - 1
      // For full-load (non-fill), don't mark done here — rebuild runs after the chunk loop
      const isFinal = isLastChunk && !(opts.syncMode === 'full' && !isFill)

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
        mergeSql: isFill
          ? MERGE_SQL_FILL_CONSTRAINTS
          : opts.syncMode === 'full'
            ? MERGE_SQL_FULL
            : MERGE_SQL,
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

    if (opts.syncMode === 'full' && !isFill) {
      await setJobStep({ jobId: exportResult.jobId, step: 'rebuild_indexes' })
      await rebuildPackageDepsIndexes()
      await setJobStep({ jobId: exportResult.jobId, step: 'rebuild_constraints' })
      await rebuildPackageDepsConstraints()
      // Finalize after rebuild — marks done with correct finishedAt
      await mergeStagingToTable({
        jobId: exportResult.jobId,
        mergeSql: [],
        tableNames: [],
        isFinal: true,
        priorRowsAffected,
        priorTableRowCounts,
      })
    }
  } catch (err) {
    if (opts.syncMode === 'full' && !isFill) {
      try {
        await setJobStep({ jobId: exportResult.jobId, step: 'rebuild_indexes' })
        await rebuildPackageDepsIndexes()
      } catch (_) {
        /* best-effort */
      }
      try {
        await setJobStep({ jobId: exportResult.jobId, step: 'rebuild_constraints' })
        await rebuildPackageDepsConstraints()
      } catch (_) {
        /* best-effort */
      }
    }
    throw err
  }
  return { rowCountBq: exportResult.rowCount }
}
