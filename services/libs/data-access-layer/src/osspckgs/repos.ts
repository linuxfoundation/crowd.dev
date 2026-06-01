import { QueryExecutor } from '../queryExecutor'
import { IDbPackageRepoUpsert, IDbRepoUpsert } from './types'

export async function findRepoIdsByUrl(
  qx: QueryExecutor,
  urls: string[],
): Promise<Map<string, number>> {
  if (urls.length === 0) return new Map()
  const rows = await qx.select(`SELECT id, url FROM repos WHERE url = ANY($(urls))`, { urls })
  return new Map(rows.map((r: { url: string; id: number }) => [r.url, r.id]))
}

/**
 * Inserts or updates a repo row keyed on url.
 * Uses COALESCE so richer data from other enrichers (GitHub, deps.dev) is never
 * overwritten with nulls from a partial write.
 * Returns the repo id.
 */
export async function upsertRepo(qx: QueryExecutor, item: IDbRepoUpsert): Promise<number> {
  const row = await qx.selectOne(
    `
    INSERT INTO repos (url, host, owner, name, last_synced_at)
    VALUES ($(url), $(host), $(owner), $(name), NOW())
    ON CONFLICT (url) DO UPDATE SET
      host           = COALESCE(EXCLUDED.host,  repos.host),
      owner          = COALESCE(EXCLUDED.owner, repos.owner),
      name           = COALESCE(EXCLUDED.name,  repos.name),
      last_synced_at = NOW()
    RETURNING id
    `,
    item,
  )
  return row.id as number
}

/**
 * Links a package to a repo with provenance metadata.
 * On conflict keeps the higher confidence value and refreshes verified_at.
 */
export async function upsertPackageRepo(
  qx: QueryExecutor,
  item: IDbPackageRepoUpsert,
): Promise<void> {
  await qx.result(
    `
    INSERT INTO package_repos (package_id, repo_id, source, confidence, verified_at)
    VALUES ($(packageId), $(repoId), $(source), $(confidence), NOW())
    ON CONFLICT (package_id, repo_id) DO UPDATE SET
      confidence  = GREATEST(EXCLUDED.confidence, package_repos.confidence),
      verified_at = NOW()
    `,
    item,
  )
}
