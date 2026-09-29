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

  return runDualPhasePrSync(ctx, async (prs, _sinceDate, onPrProcessed) => {
    for (const pullRequest of prs) {
      // a single PR's commit walk can cost up to ~304s worst case (see STATS_MAX_ATTEMPTS
      // below), so a page of PR_PAGE_SIZE PRs must check the run budget between PRs, not
      // just between pages, or a degraded provider can stall a whole page past the
      // activity's deadline with nothing checkpointed for the PRs that already finished
      if (!ctx.hasRunBudget()) {
        return
      }

      let cursor: string | null = null
      let hasMore = true
      let noStats = false
      let expectedCount: number | null = null
      let fetchedCount = 0

      while (hasMore) {
        const log = ctx.log.child({ prNumber: pullRequest.number, cursor })
        const variables = {
          owner,
          repo,
          prNumber: pullRequest.number,
          first: COMMITS_PAGE_SIZE,
          cursor,
        }
        const { page: data, usedNoStats } = await fetchCommitsPage(ctx, variables, log, noStats)
        noStats = noStats || usedNoStats

        let commits = data.repository.pullRequest?.commits
        if (!commits) {
          log.warn('github returned no commits connection for pull request')
          break
        }

        if (!noStats && commits.nodes.some((node) => !node?.commit)) {
          log.warn('github dropped commits from a diff stats page, refetching without them')
          const { page } = await fetchCommitsPage(ctx, variables, log, true)
          noStats = true
          commits = page.repository.pullRequest?.commits ?? commits
        }

        const fresh = commits.nodes
          .map((node) => node?.commit)
          .filter((commit): commit is PrCommitNode['commit'] => Boolean(commit))

        expectedCount = commits.totalCount
        fetchedCount += fresh.length

        if (fresh.length > 0) {
          await ctx.emit(fresh.map((commit) => toCommit(commit, pullRequest.id)))
        }

        hasMore = commits.pageInfo.hasNextPage
        cursor = commits.pageInfo.endCursor
      }

      if (expectedCount !== null && fetchedCount < expectedCount) {
        ctx.log.warn(
          { prNumber: pullRequest.number, expectedCount, fetchedCount },
          'fetched fewer commits than github reports for pull request',
        )
      }

      await onPrProcessed?.(pullRequest)
    }
  })
}

export const pullRequestCommitsSync: SyncDefinition = {
  name: 'pull-request-commits',
  cadenceMinutes: 720,
  schema: githubActivitySchema,
  run: runPullRequestCommitsSync,
}
