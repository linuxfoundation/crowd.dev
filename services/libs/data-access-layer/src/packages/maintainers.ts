import { QueryExecutor } from '../queryExecutor'

export interface NpmMaintainerInput {
  username: string
  displayName: string | null
  email: string | null
  role: 'author' | 'maintainer'
}

export async function upsertNpmMaintainers(
  qx: QueryExecutor,
  packageId: string,
  maintainers: NpmMaintainerInput[],
): Promise<string[]> {
  const changed = new Set<string>()

  for (const m of maintainers) {
    const row: { changed_fields: string[] } = await qx.selectOne(
      `WITH old AS (
         SELECT display_name, email FROM maintainers WHERE ecosystem = 'npm' AND username = $(username)
       ),
       ins AS (
         INSERT INTO maintainers (ecosystem, username, display_name, email)
         VALUES ('npm', $(username), $(displayName), $(email))
         ON CONFLICT (ecosystem, username) DO UPDATE SET
           display_name = COALESCE(EXCLUDED.display_name, maintainers.display_name),
           email        = COALESCE(EXCLUDED.email, maintainers.email)
         RETURNING display_name, email
       )
       SELECT array_remove(ARRAY[
         CASE WHEN o.display_name IS DISTINCT FROM ins.display_name THEN 'maintainers.display_name' END,
         CASE WHEN o.email        IS DISTINCT FROM ins.email        THEN 'maintainers.email' END
       ], NULL) AS changed_fields
       FROM ins LEFT JOIN old o ON true`,
      { username: m.username, displayName: m.displayName, email: m.email },
    )
    row.changed_fields.forEach((f) => changed.add(f))
  }

  const before: Array<{ maintainer_id: string; role: string | null }> = await qx.select(
    `SELECT maintainer_id::text AS maintainer_id, role
       FROM package_maintainers WHERE package_id = $(packageId)`,
    { packageId },
  )
  const beforeMap = new Map(before.map((r) => [r.maintainer_id, r.role]))

  await qx.result(`DELETE FROM package_maintainers WHERE package_id = $(packageId)`, { packageId })

  const afterMap = new Map<string, string | null>()
  for (const m of maintainers) {
    const row: { maintainer_id: string } | null = await qx.selectOneOrNone(
      `INSERT INTO package_maintainers (package_id, maintainer_id, role)
       SELECT $(packageId), id, $(role) FROM maintainers WHERE ecosystem = 'npm' AND username = $(username)
       ON CONFLICT (package_id, maintainer_id) DO UPDATE SET role = EXCLUDED.role
       RETURNING maintainer_id::text AS maintainer_id`,
      { packageId, role: m.role, username: m.username },
    )
    if (row) afterMap.set(row.maintainer_id, m.role)
  }

  for (const id of beforeMap.keys()) {
    if (!afterMap.has(id)) changed.add('package_maintainers.maintainer_id')
  }
  for (const [id, role] of afterMap) {
    if (!beforeMap.has(id)) changed.add('package_maintainers.maintainer_id')
    else if (beforeMap.get(id) !== role) changed.add('package_maintainers.role')
  }

  return Array.from(changed)
}
