import type { Logger } from '@crowd/logging'

import { ConnectorError } from '../../../http/errors'
import type { SyncContext, SyncDefinition, SyncOutcome } from '../../../types'
import { githubGraphql } from '../gql'
import type { PrCommitNode, PrCommitsPage } from '../graphql/pullRequestChildren'
import { PR_COMMITS_QUERY, PR_COMMITS_QUERY_NO_STATS } from '../graphql/pullRequestChildren'
import { toCommit } from '../mappers/commit'
import { parseRepoChannel } from '../paging'
import { runDualPhasePrSync } from '../prWalk'
import { githubActivitySchema } from '../schemas'

const COMMITS_PAGE_SIZE = 50

// If GitHub keeps 502ing on additions/deletions for a page (usually a commit
// with an expensive diff, e.g. a large merge), give up on the stats and take
// the page without them rather than let the whole sync stall on it.
const STATS_MAX_ATTEMPTS = 5

async function fetchCommitsPage(
  ctx: SyncContext,
  variables: Record<string, unknown>,
  log: Logger,
): Promise<PrCommitsPage> {
  try {
    return await githubGraphql<PrCommitsPage>(
      ctx.http,
      PR_COMMITS_QUERY,
      variables,
      log,
      STATS_MAX_ATTEMPTS,
    )
  } catch (err) {
    if (!(err instanceof ConnectorError) || err.errorClass !== 'provider.unavailable') {
      throw err
    }
    log.warn({ err }, 'github keeps failing on commit diff stats, retrying without them')
    return githubGraphql<PrCommitsPage>(ctx.http, PR_COMMITS_QUERY_NO_STATS, variables, log)
  }
}

async function runPullRequestCommitsSync(ctx: SyncContext): Promise<SyncOutcome> {
  const { owner, repo } = parseRepoChannel(ctx.channel.channelName)

  return runDualPhasePrSync(ctx, async (prs) => {
    for (const pullRequest of prs) {
      let cursor: string | null = null
      let hasMore = true

      while (hasMore) {
        const log = ctx.log.child({ prNumber: pullRequest.number, cursor })
        const data = await fetchCommitsPage(
          ctx,
          {
            owner,
            repo,
            prNumber: pullRequest.number,
            first: COMMITS_PAGE_SIZE,
            cursor,
          },
          log,
        )

        const commits = data.repository.pullRequest?.commits
        if (!commits) {
          break
        }

        const fresh = commits.nodes
          .map((node) => node?.commit)
          .filter((commit): commit is PrCommitNode['commit'] =>
            Boolean(commit?.author?.user?.login),
          )

        if (fresh.length > 0) {
          await ctx.emit(fresh.map((commit) => toCommit(commit, pullRequest.id)))
        }

        hasMore = commits.pageInfo.hasNextPage
        cursor = commits.pageInfo.endCursor
      }
    }
  })
}

export const pullRequestCommitsSync: SyncDefinition = {
  name: 'pull-request-commits',
  cadenceMinutes: 720,
  schema: githubActivitySchema,
  run: runPullRequestCommitsSync,
}
