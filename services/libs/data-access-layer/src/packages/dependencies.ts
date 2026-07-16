import { QueryExecutor } from '../queryExecutor'

export interface VersionDependencyEdge {
  packageId: string
  versionId: string
  dependsOnId: string
  constraint: string
  kind: 'direct' | 'dev'
}

export async function upsertVersionDependencies(
  qx: QueryExecutor,
  edges: VersionDependencyEdge[],
): Promise<number> {
  if (edges.length === 0) return 0

  const packageIds = edges.map((e) => e.packageId)
  const versionIds = edges.map((e) => e.versionId)
  const dependsOnIds = edges.map((e) => e.dependsOnId)
  const constraints = edges.map((e) => e.constraint)
  const kinds = edges.map((e) => e.kind)

  const result = await qx.result(
    `INSERT INTO package_dependencies (package_id, version_id, depends_on_id, version_constraint, dependency_kind, is_optional, created_at, updated_at)
     SELECT e.package_id, e.version_id, e.depends_on_id, e.version_constraint, e.dependency_kind, FALSE, NOW(), NOW()
       FROM unnest($(packageIds)::bigint[], $(versionIds)::bigint[], $(dependsOnIds)::bigint[],
                   $(constraints)::text[], $(kinds)::text[])
         AS e(package_id, version_id, depends_on_id, version_constraint, dependency_kind)
      ON CONFLICT (version_id, depends_on_id, dependency_kind) DO UPDATE SET
        version_constraint = EXCLUDED.version_constraint,
        updated_at         = NOW()`,
    { packageIds, versionIds, dependsOnIds, constraints, kinds },
  )
  return result
}
