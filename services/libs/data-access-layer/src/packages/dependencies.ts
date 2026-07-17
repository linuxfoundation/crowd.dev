import { QueryExecutor } from '../queryExecutor'

export interface VersionDependencyEdge {
  packageId: string
  versionId: string
  dependsOnId: string
  constraint: string
  kind: 'direct' | 'dev'
}

// Returns a batch-level changed-fields summary (like the sibling upsert* functions),
// not a per-edge diff: a batch can touch many edges across many versions in one call,
// so this reports whether ANY edge in the batch was newly inserted / had its
// constraint change, rather than one row per edge.
export async function upsertVersionDependencies(
  qx: QueryExecutor,
  edges: VersionDependencyEdge[],
): Promise<string[]> {
  if (edges.length === 0) return []

  const packageIds = edges.map((e) => e.packageId)
  const versionIds = edges.map((e) => e.versionId)
  const dependsOnIds = edges.map((e) => e.dependsOnId)
  const constraints = edges.map((e) => e.constraint)
  const kinds = edges.map((e) => e.kind)

  const row: { changed_fields: string[] } = await qx.selectOne(
    `WITH old AS (
       SELECT version_id, depends_on_id, dependency_kind, version_constraint
         FROM package_dependencies
        WHERE version_id = ANY($(versionIds)::bigint[])
          AND depends_on_id = ANY($(dependsOnIds)::bigint[])
     ),
     ins AS (
       INSERT INTO package_dependencies (package_id, version_id, depends_on_id, version_constraint, dependency_kind, is_optional, created_at, updated_at)
       SELECT e.package_id, e.version_id, e.depends_on_id, e.version_constraint, e.dependency_kind, FALSE, NOW(), NOW()
         FROM unnest($(packageIds)::bigint[], $(versionIds)::bigint[], $(dependsOnIds)::bigint[],
                     $(constraints)::text[], $(kinds)::text[])
           AS e(package_id, version_id, depends_on_id, version_constraint, dependency_kind)
        ON CONFLICT (version_id, depends_on_id, dependency_kind) DO UPDATE SET
          version_constraint = EXCLUDED.version_constraint,
          updated_at         = NOW()
       RETURNING version_id, depends_on_id, dependency_kind, version_constraint
     )
     SELECT array_remove(ARRAY[
       CASE WHEN bool_or(o.version_id IS NULL) THEN 'package_dependencies.depends_on_id' END,
       CASE WHEN bool_or(o.version_id IS NOT NULL AND o.version_constraint IS DISTINCT FROM ins.version_constraint)
            THEN 'package_dependencies.version_constraint' END
     ], NULL) AS changed_fields
     FROM ins
     LEFT JOIN old o
       ON o.version_id = ins.version_id
      AND o.depends_on_id = ins.depends_on_id
      AND o.dependency_kind = ins.dependency_kind`,
    { packageIds, versionIds, dependsOnIds, constraints, kinds },
  )
  return row.changed_fields
}
