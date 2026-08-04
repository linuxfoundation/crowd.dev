import { QueryExecutor } from '../queryExecutor'

// Run-level ledger for the packagist transitive-dependents lane (one row per run).
// The lane is a whole-ecosystem batch, so — unlike the per-purl watermarks in
// packagist_package_state — its state is a run lifecycle:
// pending → merging → done | failed.

export async function createPackagistTransitiveRun(qx: QueryExecutor): Promise<number> {
  const row = await qx.selectOne(
    `INSERT INTO packagist_transitive_runs (status) VALUES ('pending') RETURNING id`,
  )
  return row.id
}

// Newest unfinished run ('pending' OR 'merging'): a Temporal retry of the prepare
// activity must adopt the row it may have already marked merging — the activity's
// completion can be lost after the DB commit — instead of minting a duplicate and
// stranding the original. Safe because the fixed workflow id keeps the lane
// single-instance, so an unfinished row always belongs to this logical run.
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

// Backstop gate: has a run completed within the window? Failed runs don't count —
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
  await qx.result(
    `UPDATE packagist_transitive_runs
        SET status = 'merging',
            edge_count = $(edgeCount),
            packages_with_dependents = $(packagesWithDependents)
      WHERE id = $(runId)`,
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
      WHERE id = $(runId)`,
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
      WHERE id = $(runId)`,
    { runId, errorMessage },
  )
}
