import { QueryExecutor } from '../queryExecutor'

export interface NpmMaintainerInput {
  username: string
  displayName: string | null
  email: string | null
  role: 'author' | 'maintainer'
}

export async function upsertPackageMaintainers(
  qx: QueryExecutor,
  packageId: string,
  maintainers: NpmMaintainerInput[],
  ecosystem = 'npm',
): Promise<string[]> {
  const changed = new Set<string>()

  const ordered = [...maintainers].sort((a, b) =>
    a.username < b.username ? -1 : a.username > b.username ? 1 : 0,
  )

  for (const m of ordered) {
    const row: { changed_fields: string[] } = await qx.selectOne(
      `WITH old AS (
         SELECT display_name, email FROM maintainers WHERE ecosystem = $(ecosystem) AND username = $(username)
       ),
       ins AS (
         INSERT INTO maintainers (ecosystem, username, display_name, email, created_at, updated_at)
         VALUES ($(ecosystem), $(username), $(displayName), $(email), NOW(), NOW())
         ON CONFLICT (ecosystem, username) DO UPDATE SET
           display_name = COALESCE(EXCLUDED.display_name, maintainers.display_name),
           email        = COALESCE(EXCLUDED.email, maintainers.email),
           updated_at   = EXCLUDED.updated_at
         RETURNING display_name, email
       )
       SELECT array_remove(ARRAY[
         CASE WHEN o.display_name IS DISTINCT FROM ins.display_name THEN 'maintainers.display_name' END,
         CASE WHEN o.email        IS DISTINCT FROM ins.email        THEN 'maintainers.email' END
       ], NULL) AS changed_fields
       FROM ins LEFT JOIN old o ON true`,
      { ecosystem, username: m.username, displayName: m.displayName, email: m.email },
    )
    row.changed_fields.forEach((f) => changed.add(f))
  }

  const before: Array<{ maintainer_id: string; role: string | null }> = await qx.select(
    `SELECT maintainer_id::text AS maintainer_id, role
       FROM package_maintainers WHERE package_id = $(packageId)::bigint`,
    { packageId },
  )
  const beforeMap = new Map(before.map((r) => [r.maintainer_id, r.role]))

  await qx.result(`DELETE FROM package_maintainers WHERE package_id = $(packageId)::bigint`, {
    packageId,
  })

  const afterMap = new Map<string, string | null>()
  for (const m of ordered) {
    const row: { maintainer_id: string } | null = await qx.selectOneOrNone(
      `INSERT INTO package_maintainers (package_id, maintainer_id, role, created_at, updated_at)
       SELECT $(packageId)::bigint, id, $(role), NOW(), NOW() FROM maintainers WHERE ecosystem = $(ecosystem) AND username = $(username)
       ON CONFLICT (package_id, maintainer_id) DO UPDATE SET role = EXCLUDED.role, updated_at = EXCLUDED.updated_at
       RETURNING maintainer_id::text AS maintainer_id`,
      { packageId, role: m.role, username: m.username, ecosystem },
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
