import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

export interface DirectEdge {
  packageId: number
  dependsOnId: number
}

// Unique direct package→package edges for the ecosystem.
// DISTINCT deduplicates multi-version rows; parallel hint scoped to the transaction.
export async function loadDirectEdges(qx: QueryExecutor, ecosystem: string): Promise<DirectEdge[]> {
  return qx.tx(async (tx) => {
    await tx.result(`SET LOCAL max_parallel_workers_per_gather = 4`)
    return tx.select(
      `SELECT DISTINCT pd.package_id    AS "packageId",
                       pd.depends_on_id AS "dependsOnId"
         FROM package_dependencies pd
         JOIN packages p
           ON p.id        = pd.package_id
          AND p.ecosystem = $/ecosystem/
        WHERE pd.dependency_kind = 'direct'`,
      { ecosystem },
    )
  })
}

// Bulk-update centrality_score on packages_universe rows by joining through packages.
// Uses unnest — one parameterised query regardless of row count, no string interpolation.
// Isolated packages (not in the graph) remain NULL; rank_packages_universe() treats
// NULL as 0 via COALESCE. Idempotent — safe for Temporal retries.
export async function mergeCentralityScores(
  qx: QueryExecutor,
  rows: Array<{ packageId: number; centralityScore: number }>,
): Promise<void> {
  if (rows.length === 0) return
  await qx.result(
    `UPDATE packages_universe pu
        SET centrality_score = v.score
       FROM unnest($(packageIds)::bigint[], $(scores)::numeric[]) AS v(package_id, score)
       JOIN packages p ON p.id = v.package_id
      WHERE pu.purl = p.purl`,
    {
      packageIds: rows.map((r) => r.packageId),
      scores: rows.map((r) => r.centralityScore),
    },
  )
}
