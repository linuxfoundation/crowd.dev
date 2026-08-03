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

// Newest pending run, so a Temporal retry of the prepare activity reuses the row
// instead of minting one per attempt.
export async function findPendingPackagistTransitiveRun(qx: QueryExecutor): Promise<number | null> {
  const row = await qx.selectOneOrNone(
    `SELECT id FROM packagist_transitive_runs WHERE status = 'pending' ORDER BY id DESC LIMIT 1`,
  )
  return row?.id ?? null
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
