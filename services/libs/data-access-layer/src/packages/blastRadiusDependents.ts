import { QueryExecutor } from '../queryExecutor'

export interface ReverseDependentRow {
  packageId: string
  purl: string
  namespace: string | null
  name: string
  versionConstraint: string
  dependencyKind: string
  dependentCount: number | null
  transitiveDependentCount: number | null
  dependentReposCount: number | null
}

// Reverse "who depends on X" lookup, new file rather than an addition to the
// ingestion-writer dependencies.ts. Filtering on depends_on_id alone hits a single
// partition of package_dependencies (HASH-partitioned on that column); the ecosystem
// filter on packages further narrows without needing a composite index.
export async function getReverseDependents(
  qx: QueryExecutor,
  dependsOnId: string,
  ecosystem: string,
  limit: number,
): Promise<ReverseDependentRow[]> {
  const rows = await qx.select(
    `SELECT p.id AS package_id, p.purl, p.namespace, p.name,
            pd.version_constraint, pd.dependency_kind,
            p.dependent_count, p.transitive_dependent_count, p.dependent_repos_count
       FROM package_dependencies pd
       JOIN packages p ON p.id = pd.package_id
      WHERE pd.depends_on_id = $(dependsOnId)
        AND p.ecosystem = $(ecosystem)
      ORDER BY COALESCE(p.dependent_repos_count, 0) DESC,
               COALESCE(p.dependent_count, 0) DESC
      LIMIT $(limit)`,
    { dependsOnId, ecosystem, limit },
  )

  return rows.map((row: Record<string, unknown>) => ({
    packageId: row.package_id as string,
    purl: row.purl as string,
    namespace: row.namespace as string | null,
    name: row.name as string,
    versionConstraint: row.version_constraint as string,
    dependencyKind: row.dependency_kind as string,
    dependentCount: row.dependent_count as number | null,
    transitiveDependentCount: row.transitive_dependent_count as number | null,
    dependentReposCount: row.dependent_repos_count as number | null,
  }))
}

// Fallback version source when GOPROXY's @v/list can't be reached (or is rate-limited) —
// our own ingested `versions` rows for the module, already populated via deps.dev.
export async function getVersionNumbers(qx: QueryExecutor, packageId: string): Promise<string[]> {
  const rows = await qx.select(`SELECT number FROM versions WHERE package_id = $(packageId)`, {
    packageId,
  })
  return rows.map((row: { number: string }) => row.number)
}
