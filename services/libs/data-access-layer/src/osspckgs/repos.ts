import { QueryExecutor } from '../queryExecutor'
import { IDbRepoScorecardCheckUpsert, IDbRepoScorecardUpdate, IDbRepoUpsert } from './types'

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

export async function updateRepoScorecards(
  qx: QueryExecutor,
  rows: IDbRepoScorecardUpdate[],
): Promise<number> {
  if (rows.length === 0) return 0
  return qx.result(
    `
    WITH v AS (
      SELECT
        (j->>'repoId')::bigint          AS id,
        (j->>'score')::numeric(3,1)     AS score,
        (j->>'lastRunAt')::timestamptz  AS last_run_at
      FROM jsonb_array_elements($(rows)::jsonb) j
    ),
    locked AS (
      SELECT r.id
      FROM repos r
      JOIN v ON v.id = r.id
      ORDER BY r.id
      FOR NO KEY UPDATE OF r
    )
    UPDATE repos r
    SET scorecard_score       = v.score,
        scorecard_last_run_at = v.last_run_at,
        updated_at            = NOW()
    FROM v
    JOIN locked ON locked.id = v.id
    WHERE r.id = v.id
      AND (
        r.scorecard_score IS DISTINCT FROM v.score
        OR r.scorecard_last_run_at IS DISTINCT FROM v.last_run_at
      )
    `,
    { rows: JSON.stringify(rows) },
  )
}

export async function upsertRepoScorecardChecks(
  qx: QueryExecutor,
  rows: IDbRepoScorecardCheckUpsert[],
): Promise<number> {
  if (rows.length === 0) return 0
  return qx.result(
    `
    INSERT INTO repo_scorecard_checks (repo_id, check_name, score, reason)
    SELECT
      (j->>'repoId')::bigint,
      j->>'checkName',
      (j->>'score')::real,
      j->>'reason'
    FROM jsonb_array_elements($(rows)::jsonb) j
    ON CONFLICT (repo_id, check_name) DO UPDATE SET
      score      = EXCLUDED.score,
      reason     = EXCLUDED.reason,
      updated_at = NOW()
    WHERE repo_scorecard_checks.score IS DISTINCT FROM EXCLUDED.score
       OR repo_scorecard_checks.reason IS DISTINCT FROM EXCLUDED.reason
    `,
    { rows: JSON.stringify(rows) },
  )
}
