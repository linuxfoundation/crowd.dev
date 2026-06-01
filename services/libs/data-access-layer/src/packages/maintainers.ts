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
): Promise<void> {
  for (const m of maintainers) {
    await qx.result(
      `INSERT INTO maintainers (ecosystem, username, display_name, email)
       VALUES ('npm', $(username), $(displayName), $(email))
       ON CONFLICT (ecosystem, username) DO UPDATE SET
         display_name = COALESCE(EXCLUDED.display_name, maintainers.display_name),
         email        = COALESCE(EXCLUDED.email, maintainers.email)`,
      { username: m.username, displayName: m.displayName, email: m.email },
    )
  }

  await qx.result(`DELETE FROM package_maintainers WHERE package_id = $(packageId)`, { packageId })

  for (const m of maintainers) {
    await qx.result(
      `INSERT INTO package_maintainers (package_id, maintainer_id, role)
       SELECT $(packageId), id, $(role) FROM maintainers WHERE ecosystem = 'npm' AND username = $(username)`,
      { packageId, role: m.role, username: m.username },
    )
  }
}
