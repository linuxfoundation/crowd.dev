import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { LightRepoResult } from './types'

export async function bulkUpdateEnrichedRepos(
  qx: QueryExecutor,
  rows: LightRepoResult[],
): Promise<void> {
  if (rows.length === 0) return

  // Single round-trip: unpack rows from JSON, update all in one query.
  // NULLIF handles empty strings from JSON serialisation of null timestamps.
  // Nullable columns (stars, forks, etc.) stay null when GitHub returns nothing.
  await qx.result(
    `
    UPDATE repos AS r
    SET
      description      = v.description,
      primary_language = v.primary_language,
      topics           = v.topics,
      stars            = v.stars::int,
      forks            = v.forks::int,
      watchers         = v.watchers::int,
      open_issues      = v.open_issues::int,
      last_commit_at          = NULLIF(v.last_commit_at, '')::timestamptz,
      archived                = v.archived::bool,
      disabled                = v.disabled::bool,
      is_fork                 = v.is_fork::bool,
      security_policy_enabled = v.security_policy_enabled::bool,
      security_file_enabled   = v.security_file_enabled::bool,
      host             = COALESCE(r.host,       v.host),
      owner            = COALESCE(r.owner,      v.owner),
      name             = COALESCE(r.name,       v.name),
      created_at       = COALESCE(r.created_at, NULLIF(v.created_at, '')::timestamptz),
      last_synced_at   = NOW()
    FROM (
      SELECT
        j->>'url'                                             AS url,
        j->>'description'                                     AS description,
        j->>'primaryLanguage'                                 AS primary_language,
        ARRAY(SELECT jsonb_array_elements_text(j->'topics')) AS topics,
        j->>'stars'                                           AS stars,
        j->>'forks'                                           AS forks,
        j->>'watchers'                                        AS watchers,
        j->>'openIssues'                                      AS open_issues,
        j->>'lastCommitAt'                                    AS last_commit_at,
        j->>'archived'                                        AS archived,
        j->>'disabled'                                        AS disabled,
        j->>'isFork'                                          AS is_fork,
        j->>'securityPolicyEnabled'                           AS security_policy_enabled,
        j->>'securityFileEnabled'                             AS security_file_enabled,
        j->>'host'                                            AS host,
        j->>'owner'                                           AS owner,
        j->>'name'                                            AS name,
        j->>'createdAt'                                       AS created_at
      FROM jsonb_array_elements($1::jsonb) j
    ) v
    WHERE r.url = v.url
    `,
    [JSON.stringify(rows)],
  )
}

export async function markReposSkipped(qx: QueryExecutor, urls: string[]): Promise<void> {
  if (urls.length === 0) return

  await qx.result(
    `
    UPDATE repos
    SET
      last_synced_at  = NOW(),
      skip_enrichment = true
    WHERE url = ANY($1::text[])
    `,
    [urls],
  )
}
