import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

import { LightRepoResult } from './types'

const log = getServiceChildLogger('github-repos-enricher:update')

export async function updateEnrichedRepos(
  qx: QueryExecutor,
  rows: LightRepoResult[],
): Promise<void> {
  if (rows.length === 0) return

  for (const r of rows) {
    await qx.result(
      `UPDATE repos SET
        host             = COALESCE(host, $(host)),
        owner            = COALESCE(owner, $(owner)),
        name             = COALESCE(name, $(name)),
        description      = $(description),
        primary_language = $(primaryLanguage),
        topics           = $(topics)::text[],
        stars            = $(stars),
        forks            = $(forks),
        watchers         = $(watchers),
        open_issues      = $(openIssues),
        last_commit_at   = $(lastCommitAt)::timestamptz,
        archived         = $(archived),
        disabled         = $(disabled),
        is_fork          = $(isFork),
        created_at       = COALESCE(created_at, $(createdAt)::timestamptz),
        last_synced_at   = NOW()
       WHERE url = $(url)`,
      r,
    )
  }

  log.debug({ count: rows.length }, 'Updated enriched repos')
}
