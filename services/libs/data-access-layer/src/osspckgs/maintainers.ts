import { QueryExecutor } from '../queryExecutor'

import { IDbMaintainerUpsert, IDbPackageMaintainerUpsert } from './types'

/**
 * Inserts or updates a maintainer row.
 * Returns the maintainer id.
 */
export async function upsertMaintainer(
  qx: QueryExecutor,
  item: IDbMaintainerUpsert,
): Promise<number> {
  const row = await qx.selectOne(
    `
    INSERT INTO maintainers (
      ecosystem,
      username,
      display_name,
      url,
      email_hash
    ) VALUES (
      $(ecosystem),
      $(username),
      $(displayName),
      $(url),
      $(emailHash)
    )
    ON CONFLICT (ecosystem, username) DO UPDATE SET
      display_name = COALESCE(EXCLUDED.display_name, maintainers.display_name),
      url          = COALESCE(EXCLUDED.url,          maintainers.url),
      email_hash   = COALESCE(EXCLUDED.email_hash,   maintainers.email_hash)
    RETURNING id
    `,
    item,
  )
  return row.id as number
}

/**
 * Links a maintainer to a package with the given role.
 * Does nothing on conflict.
 */
export async function upsertPackageMaintainer(
  qx: QueryExecutor,
  item: IDbPackageMaintainerUpsert,
): Promise<void> {
  await qx.result(
    `
    INSERT INTO package_maintainers (package_id, maintainer_id, role)
    VALUES ($(packageId), $(maintainerId), $(role))
    ON CONFLICT (package_id, maintainer_id) DO NOTHING
    `,
    item,
  )
}
