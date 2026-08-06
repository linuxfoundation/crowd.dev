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
 * `last_synced_at` is intentionally NOT touched here — that column is owned by the
 * GitHub enricher as its freshness signal. Maven discovery only stamps updated_at.
 * Returns the repo id.
 */
export async function upsertRepo(qx: QueryExecutor, item: IDbRepoUpsert): Promise<number> {
  const row = await qx.selectOne(
    `
    INSERT INTO repos (url, host, owner, name, updated_at)
    VALUES ($(url), $(host), $(owner), $(name), NOW())
    ON CONFLICT (url) DO UPDATE SET
      host       = COALESCE(EXCLUDED.host,  repos.host),
      owner      = COALESCE(EXCLUDED.owner, repos.owner),
      name       = COALESCE(EXCLUDED.name,  repos.name),
      updated_at = NOW()
    RETURNING id
    `,
    item,
  )
  return row.id as number
}

/**
 * Removes the `declared` repo links for the given packages. Used by the
 * repository_url backfill to drop stale links when a package's canonical URL
 * changes or is cleared, keeping package_repos consistent with
 * packages.repository_url. Only touches source='declared' (the link Maven
 * owns) — links from other enrichers (deps.dev, heuristic, manual) are left
 * untouched. The shared `repos` rows are never deleted.
 */
export async function deleteMavenPackageRepoLinks(
  qx: QueryExecutor,
  packageIds: number[],
): Promise<void> {
  if (packageIds.length === 0) return
  await qx.result(
    `DELETE FROM package_repos
     WHERE package_id = ANY($(packageIds)::bigint[]) AND source = 'declared'`,
    { packageIds },
  )
}

/**
 * Links a package to a repo with provenance metadata.
 * On conflict keeps the higher confidence value and refreshes verified_at.
 * Returns the list of fields that actually changed.
 */
export async function upsertMavenPackageRepo(
  qx: QueryExecutor,
  item: IDbPackageRepoUpsert,
): Promise<string[]> {
  const row: { changed_fields: string[] } = await qx.selectOne(
    `
    WITH old AS (
      SELECT source, confidence FROM package_repos
       WHERE package_id = $(packageId) AND repo_id = $(repoId)
    ),
    ins AS (
      INSERT INTO package_repos (package_id, repo_id, source, confidence, verified_at, created_at)
      VALUES ($(packageId), $(repoId), $(source), $(confidence), NOW(), NOW())
      ON CONFLICT (package_id, repo_id) DO UPDATE SET
        confidence  = GREATEST(EXCLUDED.confidence, package_repos.confidence),
        verified_at = NOW()
      RETURNING source, confidence
    )
    SELECT array_remove(ARRAY[
      CASE WHEN o.source IS NULL                              THEN 'package_repos.repo_id' END,
      CASE WHEN o.source IS NULL                              THEN 'package_repos.source' END,
      CASE WHEN o.source IS NULL
             OR o.confidence IS DISTINCT FROM ins.confidence THEN 'package_repos.confidence' END
    ], NULL) AS changed_fields
    FROM ins LEFT JOIN old o ON true
    `,
    item,
  )
  return row.changed_fields
}
