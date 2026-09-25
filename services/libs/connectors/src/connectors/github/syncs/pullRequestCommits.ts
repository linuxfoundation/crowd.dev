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

const STATS_MAX_ATTEMPTS = 2
const NO_STATS_MAX_ATTEMPTS = 3
// Worst case (2 stats + 3 no-stats attempts, full backoff) is ~304s — under
// the 600s activity timeout budget (client.ts MAX_ATTEMPTS docs the math).

interface CommitsPageResult {
  page: PrCommitsPage
  usedNoStats: boolean
}

async function fetchCommitsPage(
  ctx: SyncContext,
  variables: Record<string, unknown>,
  log: Logger,
  skipStats: boolean,
): Promise<CommitsPageResult> {
  if (skipStats) {
    const page = await githubGraphql<PrCommitsPage>(
      ctx.http,
      PR_COMMITS_QUERY_NO_STATS,
      variables,
      log,
      NO_STATS_MAX_ATTEMPTS,
    )
    return { page, usedNoStats: true }
  }

  try {
    const page = await githubGraphql<PrCommitsPage>(
      ctx.http,
      PR_COMMITS_QUERY,
      variables,
      log,
      STATS_MAX_ATTEMPTS,
    )
    return { page, usedNoStats: false }
  } catch (err) {
    if (!(err instanceof ConnectorError) || err.errorClass !== 'provider.unavailable') {
      throw err
    }
    log.warn({ err }, 'github keeps failing on commit diff stats, retrying without them')
    const page = await githubGraphql<PrCommitsPage>(
      ctx.http,
      PR_COMMITS_QUERY_NO_STATS,
      variables,
      log,
      NO_STATS_MAX_ATTEMPTS,
    )
    return { page, usedNoStats: true }
  }
}

async function runPullRequestCommitsSync(ctx: SyncContext): Promise<SyncOutcome> {
  const { owner, repo } = parseRepoChannel(ctx.channel.channelName)

  return runDualPhasePrSync(ctx, async (prs) => {
    for (const pullRequest of prs) {
      let cursor: string | null = null
      let hasMore = true
      let noStats = false

      while (hasMore) {
        const log = ctx.log.child({ prNumber: pullRequest.number, cursor })
        const { page: data, usedNoStats } = await fetchCommitsPage(
          ctx,
          {
            owner,
            repo,
            prNumber: pullRequest.number,
            first: COMMITS_PAGE_SIZE,
            cursor,
          },
          log,
          noStats,
        )
        noStats = noStats || usedNoStats

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
