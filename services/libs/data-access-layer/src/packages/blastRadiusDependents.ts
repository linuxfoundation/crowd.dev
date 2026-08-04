import { QueryExecutor } from '../queryExecutor'

export interface ReverseDependentRow {
  packageId: string
  purl: string
  namespace: string | null
  name: string
  versionNumber: string
  versionConstraint: string
  dependencyKind: string
  dependentCount: number | null
  // bigint column — the packages DB connection only overrides the NUMERIC/int4 type
  // parsers (see @crowd/database connection.ts), so int8 comes back as a string.
  transitiveDependentCount: string | null
  dependentReposCount: number | null
}

// Reverse "who depends on X" lookup, new file rather than an addition to the
// ingestion-writer dependencies.ts. Filtering on depends_on_id alone hits a single
// partition of package_dependencies (HASH-partitioned on that column); the ecosystem
// filter on packages further narrows without needing a composite index.
//
// package_dependencies is unique per (version_id, depends_on_id, dependency_kind), not
// per depends_on_id alone — a package with many historical versions that all require
// the vulnerable package would otherwise surface as multiple rows and could consume the
// whole LIMIT. DISTINCT ON (p.id) collapses to one concrete version per dependent package
// first, preferring its latest ingested version, before ranking/limiting.
export async function getReverseDependents(
  qx: QueryExecutor,
  dependsOnId: string,
  ecosystem: string,
  limit: number,
): Promise<ReverseDependentRow[]> {
  const rows = await qx.select(
    `WITH deduped AS (
       SELECT DISTINCT ON (p.id)
              p.id AS package_id, p.purl, p.namespace, p.name,
              v.number AS version_number,
              pd.version_constraint, pd.dependency_kind,
              p.dependent_count, p.transitive_dependent_count, p.dependent_repos_count
         FROM package_dependencies pd
         JOIN packages p ON p.id = pd.package_id
         JOIN versions v ON v.id = pd.version_id AND v.package_id = pd.package_id
        WHERE pd.depends_on_id = $(dependsOnId)
          AND p.ecosystem = $(ecosystem)
        ORDER BY p.id, v.is_latest DESC NULLS LAST, v.published_at DESC NULLS LAST
     )
     SELECT * FROM deduped
     ORDER BY COALESCE(dependent_repos_count, 0) DESC,
              COALESCE(dependent_count, 0) DESC
     LIMIT $(limit)`,
    { dependsOnId, ecosystem, limit },
  )

  return rows.map((row: Record<string, unknown>) => ({
    packageId: row.package_id as string,
    purl: row.purl as string,
    namespace: row.namespace as string | null,
    name: row.name as string,
    versionNumber: row.version_number as string,
    versionConstraint: row.version_constraint as string,
    dependencyKind: row.dependency_kind as string,
    dependentCount: row.dependent_count as number | null,
    transitiveDependentCount: row.transitive_dependent_count as string | null,
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
