import { getServiceChildLogger } from '@crowd/logging'

// import { formatQuery, QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { LightRepoResult } from './types'

const log = getServiceChildLogger('sync-light-repos:upsert')

export async function upsertLightRepos(_qx: QueryExecutor, rows: LightRepoResult[]): Promise<void> {
  if (rows.length === 0) return

  log.info({ count: rows.length, rows: JSON.stringify(rows, null, 2) }, 'upsert results')

  // const values = rows
  //   .map((r) =>
  //     formatQuery(
  //       `($(url), $(host), $(owner), $(name), $(description), $(primaryLanguage), $(topics)::text[],
  //         $(stars), $(forks), $(watchers), $(openIssues), $(lastCommitAt)::timestamptz,
  //         $(archived), $(disabled), $(isFork), $(createdAt)::timestamptz)`,
  //       r,
  //     ),
  //   )
  //   .join(',\n')

  // await _qx.result(`
  //   INSERT INTO repos (
  //     url, host, owner, name, description, primary_language, topics,
  //     stars, forks, watchers, open_issues, last_commit_at,
  //     archived, disabled, is_fork, created_at, last_synced_at
  //   ) VALUES ${values}
  //   ON CONFLICT (url) DO UPDATE SET
  //     host             = EXCLUDED.host,
  //     owner            = EXCLUDED.owner,
  //     name             = EXCLUDED.name,
  //     description      = EXCLUDED.description,
  //     primary_language = EXCLUDED.primary_language,
  //     topics           = EXCLUDED.topics,
  //     stars            = EXCLUDED.stars,
  //     forks            = EXCLUDED.forks,
  //     watchers         = EXCLUDED.watchers,
  //     open_issues      = EXCLUDED.open_issues,
  //     last_commit_at   = EXCLUDED.last_commit_at,
  //     archived         = EXCLUDED.archived,
  //     disabled         = EXCLUDED.disabled,
  //     is_fork          = EXCLUDED.is_fork,
  //     last_synced_at   = NOW()
  // `)
}
