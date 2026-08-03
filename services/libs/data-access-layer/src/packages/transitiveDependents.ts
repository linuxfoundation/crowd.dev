import { QueryExecutor } from '../queryExecutor'

export interface PackagistTransitiveMergeResult {
  processed: number
  changed: number
  nextCursor: string
}

// Typed so the activity layer can classify it non-retryable: an empty counts table
// cannot heal by retrying the merge.
export class EmptyPackagistTransitiveCountsError extends Error {
  constructor() {
    super(
      'staging.packagist_transitive_counts is empty — refusing to zero-fill packages.transitive_dependent_count (truncated by a crash mid-drain?)',
    )
    this.name = 'EmptyPackagistTransitiveCountsError'
  }
}

// Shared scaffold for the two staging builders. The CTAS command tag already carries the
// row count, so no separate COUNT(*) rescan is needed. ANALYZE matters: a just-created
// table has no pg_statistic rows, and both consumers (the recursive closure, the 45-odd
// merge-batch joins) would otherwise plan against default selectivity guesses.
async function rebuildStagingTable(
  qx: QueryExecutor,
  table: string,
  createAsSql: string,
  indexColumns: string,
): Promise<number> {
  return qx.tx(async (tx) => {
    await tx.result(`SET LOCAL max_parallel_workers_per_gather = 4`)
    // Below the activity's 45-min deadline: Temporal timeouts don't kill an in-flight
    // statement, so without this a timed-out CTAS would keep running alongside its retry.
    await tx.result(`SET LOCAL statement_timeout = '40min'`)
    await tx.result(`DROP TABLE IF EXISTS ${table}`)
    const rows = await tx.result(`CREATE UNLOGGED TABLE ${table} AS ${createAsSql}`)
    await tx.result(`CREATE INDEX ON ${table} (${indexColumns})`)
    await tx.result(`ANALYZE ${table}`)
    return rows
  })
}

// Collapses version-level direct requires into distinct package-level pairs.
// dep = requirer, subj = depended-upon — same naming as the GO closure script in
// packages_worker/src/deps-dev/queries/dependentCountsSql.ts.
// The only query here that touches the ~1.5B-row package_dependencies table; package_id
// has no index, so this is a deliberate weekly parallel seq scan.
export async function snapshotPackagistDirectEdges(qx: QueryExecutor): Promise<number> {
  return rebuildStagingTable(
    qx,
    'staging.packagist_transitive_edges',
    `SELECT DISTINCT pd.package_id AS dep, pd.depends_on_id AS subj
       FROM package_dependencies pd
       JOIN packages p
         ON p.id = pd.package_id
        AND p.ecosystem = 'packagist'
      WHERE pd.dependency_kind = 'direct'
        AND pd.package_id != pd.depends_on_id`,
    'subj, dep',
  )
}

// Reverse transitive closure over the snapshot: one row per package with ≥1 dependent,
// transitive = distinct reach minus distinct direct. The snapshot excludes self-edges,
// but cycles re-introduce (subj, subj) pairs in reach — hence the dep != subj filter.
export async function computePackagistTransitiveCounts(qx: QueryExecutor): Promise<number> {
  return rebuildStagingTable(
    qx,
    'staging.packagist_transitive_counts',
    `WITH RECURSIVE reach(subj, dep) AS (
       SELECT subj, dep FROM staging.packagist_transitive_edges
       UNION
       SELECT r.subj, e.dep
         FROM reach r
         JOIN staging.packagist_transitive_edges e ON e.subj = r.dep
     ),
     direct AS (SELECT subj, COUNT(*) AS n FROM staging.packagist_transitive_edges GROUP BY subj),
     total AS (SELECT subj, COUNT(*) AS n FROM reach WHERE dep != subj GROUP BY subj)
     SELECT t.subj AS package_id, t.n - d.n AS transitive_dependent_count
       FROM total t
       JOIN direct d USING (subj)`,
    'package_id',
  )
}

// One keyset batch of packagist package ids merged from the counts staging table.
// COALESCE zero-fills packages with no dependents ("computed, none" vs NULL "never
// computed"); IS DISTINCT FROM keeps re-runs churn-free for Sequin/Tinybird. An empty
// afterId means "from the start" so first-generation callers don't need a sentinel.
export async function mergePackagistTransitiveCounts(
  qx: QueryExecutor,
  afterId: string,
  limit: number,
): Promise<PackagistTransitiveMergeResult> {
  // The zero-fill makes an empty counts table indistinguishable from "every package is
  // a leaf" — and the table is UNLOGGED, so a crash-recovery truncation mid-drain would
  // otherwise silently wipe every remaining count. A non-empty closure output is
  // guaranteed by prepare's own empty-snapshot abort, so empty here is always an error.
  const guard = await qx.selectOne(
    `SELECT EXISTS (SELECT 1 FROM staging.packagist_transitive_counts) AS populated`,
  )
  if (!guard.populated) {
    throw new EmptyPackagistTransitiveCountsError()
  }

  const row = await qx.selectOne(
    `WITH batch AS (
       SELECT id
         FROM packages
        WHERE ecosystem = 'packagist'
          AND id > COALESCE(NULLIF($(afterId), ''), '0')::bigint
        ORDER BY id
        LIMIT $(limit)
     ),
     updated AS (
       UPDATE packages p
          SET transitive_dependent_count = COALESCE(c.transitive_dependent_count, 0),
              last_synced_at = NOW()
         FROM batch b
         LEFT JOIN staging.packagist_transitive_counts c ON c.package_id = b.id
        WHERE p.id = b.id
          AND p.transitive_dependent_count IS DISTINCT FROM COALESCE(c.transitive_dependent_count, 0)
        RETURNING p.id
     )
     SELECT (SELECT COUNT(*) FROM batch) AS processed,
            (SELECT COUNT(*) FROM updated) AS changed,
            COALESCE((SELECT MAX(id)::text FROM batch), '') AS next_cursor`,
    { afterId, limit },
  )
  return {
    processed: Number(row.processed),
    changed: Number(row.changed),
    nextCursor: row.next_cursor,
  }
}
