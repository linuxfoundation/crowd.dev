import { QueryExecutor } from '../queryExecutor'

// Run-level ledger for the packagist transitive lane, one row per run:
// pending → merging → done | failed. A whole-ecosystem batch has no per-purl outcomes.

export async function createPackagistTransitiveRun(qx: QueryExecutor): Promise<number> {
  const row = await qx.selectOne(
    `INSERT INTO packagist_transitive_runs (status) VALUES ('pending') RETURNING id`,
  )
  return row.id
}

// A prepare retry must adopt the row it may have already marked 'merging'
// (completion can be lost post-commit) instead of minting a stranded duplicate.
export async function findUnfinishedPackagistTransitiveRun(
  qx: QueryExecutor,
): Promise<number | null> {
  const row = await qx.selectOneOrNone(
    `SELECT id FROM packagist_transitive_runs
      WHERE status IN ('pending', 'merging')
      ORDER BY id DESC LIMIT 1`,
  )
  return row?.id ?? null
}

// Backstop gate: has a run completed within the window? Failed runs don't count;
// the backstop exists precisely to retry after a broken week.
export async function hasRecentDonePackagistTransitiveRun(
  qx: QueryExecutor,
  withinDays: number,
): Promise<boolean> {
  const row = await qx.selectOne(
    `SELECT EXISTS (
       SELECT 1 FROM packagist_transitive_runs
        WHERE status = 'done'
          AND finished_at > NOW() - $(withinDays) * INTERVAL '1 day'
     ) AS recent`,
    { withinDays },
  )
  return Boolean(row.recent)
}

export async function markPackagistTransitiveRunMerging(
  qx: QueryExecutor,
  runId: number,
  graph: { edgeCount: number; packagesWithDependents: number },
): Promise<void> {
  // Guarded transitions make terminal states absorbing: a zombie attempt that outlived
  // its Temporal timeout can never revive a run another attempt already finished/failed.
  await qx.result(
    `UPDATE packagist_transitive_runs
        SET status = 'merging',
            edge_count = $(edgeCount),
            packages_with_dependents = $(packagesWithDependents)
      WHERE id = $(runId)
        AND status IN ('pending', 'merging')`,
    { runId, edgeCount: graph.edgeCount, packagesWithDependents: graph.packagesWithDependents },
  )
}

export async function finishPackagistTransitiveRun(
  qx: QueryExecutor,
  runId: number,
  totals: { processed: number; changed: number },
): Promise<void> {
  await qx.result(
    `UPDATE packagist_transitive_runs
        SET status = 'done',
            processed_rows = $(processed),
            changed_rows = $(changed),
            finished_at = NOW()
      WHERE id = $(runId)
        AND status = 'merging'`,
    { runId, processed: totals.processed, changed: totals.changed },
  )
}

export async function failPackagistTransitiveRun(
  qx: QueryExecutor,
  runId: number,
  errorMessage: string,
): Promise<void> {
  await qx.result(
    `UPDATE packagist_transitive_runs
        SET status = 'failed',
            error_message = $(errorMessage),
            finished_at = NOW()
      WHERE id = $(runId)
        AND status IN ('pending', 'merging')`,
    { runId, errorMessage },
  )
}
