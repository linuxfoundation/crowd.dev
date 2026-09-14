import type { SyncContext, SyncOutcome } from '../../types'

import { githubGraphql } from './gql'
import type { PullRequestNode, PullRequestsPage } from './graphql/pullRequests'
import { PULL_REQUESTS_QUERY } from './graphql/pullRequests'
import { parseRepoChannel, readWatermark } from './paging'

export const PR_PAGE_SIZE = 50

export type PrPageHandler = (prs: PullRequestNode[], sinceDate: Date | null) => Promise<void>

async function runBackfill(
  ctx: SyncContext,
  owner: string,
  repo: string,
  processPrs: PrPageHandler,
): Promise<SyncOutcome> {
  const watermark = readWatermark(ctx.watermark)
  let cursor = watermark.cursor
  let since = watermark.since

  while (ctx.hasRunBudget()) {
    const data = await githubGraphql<PullRequestsPage>(ctx.http, PULL_REQUESTS_QUERY, {
      owner,
      repo,
      first: PR_PAGE_SIZE,
      cursor,
      direction: 'ASC',
    })

    const { pageInfo, nodes } = data.repository.pullRequests
    const pullRequests = nodes.filter((node): node is PullRequestNode => node !== null)

    if (pullRequests.length > 0) {
      await processPrs(pullRequests, null)
      since = pullRequests[pullRequests.length - 1].updatedAt
    }

    if (!pageInfo.hasNextPage) {
      await ctx.commitWatermark({ phase: 'incremental', since, cursor: null })
      return { complete: true }
    }

    cursor = pageInfo.endCursor
    await ctx.commitWatermark({ phase: 'backfill', since, cursor })
  }

  return { complete: false }
}

async function runIncremental(
  ctx: SyncContext,
  owner: string,
  repo: string,
  since: string,
  processPrs: PrPageHandler,
): Promise<SyncOutcome> {
  const sinceDate = new Date(since)
  let cursor: string | null = null
  let newSince: string | null = null

  while (ctx.hasRunBudget()) {
    const data = await githubGraphql<PullRequestsPage>(ctx.http, PULL_REQUESTS_QUERY, {
      owner,
      repo,
      first: PR_PAGE_SIZE,
      cursor,
      direction: 'DESC',
    })

    const { pageInfo, nodes } = data.repository.pullRequests
    const pullRequests = nodes.filter((node): node is PullRequestNode => node !== null)

    if (newSince === null && pullRequests.length > 0) {
      newSince = pullRequests[0].updatedAt
    }

    const fresh = pullRequests.filter((pr) => new Date(pr.updatedAt) > sinceDate)
    if (fresh.length > 0) {
      await processPrs(fresh, sinceDate)
    }

    const reachedSince = fresh.length < pullRequests.length
    if (reachedSince || !pageInfo.hasNextPage) {
      await ctx.commitWatermark({ phase: 'incremental', since: newSince ?? since, cursor: null })
      return { complete: true }
    }

    cursor = pageInfo.endCursor
  }

  return { complete: false }
}

export async function runDualPhasePrSync(
  ctx: SyncContext,
  processPrs: PrPageHandler,
): Promise<SyncOutcome> {
  const { owner, repo } = parseRepoChannel(ctx.channel.channelName)
  const watermark = readWatermark(ctx.watermark)

  if (watermark.phase === 'incremental' && watermark.since) {
    return runIncremental(ctx, owner, repo, watermark.since, processPrs)
  }
  return runBackfill(ctx, owner, repo, processPrs)
}
