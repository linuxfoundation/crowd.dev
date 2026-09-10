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

// The CTAS command tag already carries the row count. ANALYZE matters: a fresh
// table has no pg_statistic rows, so consumers would plan on default selectivity.
async function rebuildStagingTable(
  qx: QueryExecutor,
  table: string,
  createAsSql: string,
  indexColumns: string,
): Promise<number> {
  return qx.tx(async (tx) => {
    await tx.result(`SET LOCAL max_parallel_workers_per_gather = 4`)
    // Temporal timeouts don't kill in-flight SQL; this bounds each statement so a hung
    // CTAS dies well inside the 90-min activity deadline (2 statements + slack).
    await tx.result(`SET LOCAL statement_timeout = '40min'`)
    await tx.result(`DROP TABLE IF EXISTS ${table}`)
    const rows = await tx.result(`CREATE UNLOGGED TABLE ${table} AS ${createAsSql}`)
    await tx.result(`CREATE INDEX ON ${table} (${indexColumns})`)
    await tx.result(`ANALYZE ${table}`)
    return rows
  })
}

// dep = requirer, subj = depended-upon (GO closure script naming). package_id has
// no index on the ~1.5B-row table; the weekly seq scan is deliberate.
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

// One row per package with ≥1 dependent; transitive = reach minus direct. Cycles
// re-introduce (subj, subj) pairs, hence the dep != subj filter.
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

// COALESCE zero-fills dependent-less packages ("computed, none" vs NULL "never
// computed"); IS DISTINCT FROM keeps re-runs churn-free. Empty afterId means "from the start".
export async function mergePackagistTransitiveCounts(
  qx: QueryExecutor,
  afterId: string,
  limit: number,
): Promise<PackagistTransitiveMergeResult> {
  // An empty counts table reads as "all leaves", and UNLOGGED tables truncate on
  // crash recovery; prepare guarantees non-empty output, so empty is always an error.
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
