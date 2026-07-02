import { QueryExecutor } from '../queryExecutor'

import { IDbMaintainerUpsert, IDbPackageMaintainerUpsert } from './types'

/**
 * Inserts or updates a maintainer row.
 * Returns the maintainer id and the list of fields that actually changed.
 */
export async function upsertMaintainer(
  qx: QueryExecutor,
  item: IDbMaintainerUpsert,
): Promise<{ id: number; changedFields: string[] }> {
  const row = await qx.selectOne(
    `
    WITH old AS (
      SELECT display_name, url, email, github_login
        FROM maintainers WHERE ecosystem = $(ecosystem) AND username = $(username)
    ),
    ins AS (
      INSERT INTO maintainers (ecosystem, username, display_name, url, email, github_login, created_at, updated_at)
      VALUES ($(ecosystem), $(username), $(displayName), $(url), $(email), $(githubLogin), NOW(), NOW())
      ON CONFLICT (ecosystem, username) DO UPDATE SET
        display_name = COALESCE(EXCLUDED.display_name, maintainers.display_name),
        url          = COALESCE(EXCLUDED.url,          maintainers.url),
        email        = COALESCE(EXCLUDED.email,        maintainers.email),
        github_login = COALESCE(EXCLUDED.github_login, maintainers.github_login),
        updated_at   = NOW()
      RETURNING id, display_name, url, email, github_login
    )
    SELECT ins.id,
           array_remove(ARRAY[
             CASE WHEN o.display_name IS DISTINCT FROM ins.display_name THEN 'maintainers.display_name' END,
             CASE WHEN o.url          IS DISTINCT FROM ins.url          THEN 'maintainers.url' END,
             CASE WHEN o.email        IS DISTINCT FROM ins.email        THEN 'maintainers.email' END,
             CASE WHEN o.github_login IS DISTINCT FROM ins.github_login THEN 'maintainers.github_login' END
           ], NULL) AS changed_fields
    FROM ins LEFT JOIN old o ON true
    `,
    { ...item, githubLogin: item.githubLogin ?? null },
  )
  return { id: row.id as number, changedFields: row.changed_fields as string[] }
}

/**
 * Inserts a package→maintainer link if it doesn't already exist.
 * Returns the id of the newly inserted row, or null if it already existed.
 * Non-destructive: never deletes or modifies existing links.
 */
export async function insertPackageMaintainerLink(
  qx: QueryExecutor,
  packageId: number,
  maintainerId: number,
  role: 'author' | 'maintainer' | null,
  ingestionSource?: string | null,
): Promise<number | null> {
  const row = await qx.selectOneOrNone(
    `INSERT INTO package_maintainers (package_id, maintainer_id, role, ingestion_source, created_at, updated_at)
     VALUES ($(packageId), $(maintainerId), $(role), $(ingestionSource), NOW(), NOW())
     ON CONFLICT (package_id, maintainer_id) DO NOTHING
     RETURNING id`,
    { packageId, maintainerId, role, ingestionSource: ingestionSource ?? null },
  )
  return row?.id ?? null
}

/**
 * Replaces all maintainer links for a package with the given list.
 * Deletes links that are no longer present and inserts/updates new ones.
 * Returns the list of fields that changed (additions, removals, role changes).
 */
export async function replacePackageMaintainers(
  qx: QueryExecutor,
  packageId: number,
  links: Array<Pick<IDbPackageMaintainerUpsert, 'maintainerId' | 'role'>>,
): Promise<string[]> {
  const before: Array<{ maintainer_id: number; role: string | null }> = await qx.select(
    `SELECT maintainer_id, role FROM package_maintainers WHERE package_id = $(packageId)`,
    { packageId },
  )
  const beforeMap = new Map(before.map((r) => [r.maintainer_id, r.role]))

  await qx.result(`DELETE FROM package_maintainers WHERE package_id = $(packageId)`, { packageId })

  const afterMap = new Map<number, string | null>()
  for (const { maintainerId, role } of links) {
    await qx.result(
      `INSERT INTO package_maintainers (package_id, maintainer_id, role, created_at, updated_at)
       VALUES ($(packageId), $(maintainerId), $(role), NOW(), NOW())
       ON CONFLICT (package_id, maintainer_id) DO UPDATE SET role = EXCLUDED.role, updated_at = NOW()`,
      { packageId, maintainerId, role },
    )
    afterMap.set(maintainerId, role)
  }

  const changed = new Set<string>()
  for (const id of beforeMap.keys()) {
    if (!afterMap.has(id)) changed.add('package_maintainers.maintainer_id')
  }
  for (const [id, role] of afterMap) {
    if (!beforeMap.has(id)) changed.add('package_maintainers.maintainer_id')
    else if (beforeMap.get(id) !== role) changed.add('package_maintainers.role')
  }

  return Array.from(changed)
}
