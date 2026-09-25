import type { SyncContext, SyncOutcome } from '../../types'
import { githubGraphql } from './gql'
import type { PullRequestNode, PullRequestsPage } from './graphql/pullRequests'
import { PULL_REQUESTS_QUERY } from './graphql/pullRequests'
import type { CoveredWindow } from './paging'
import { parseRepoChannel, readWatermark } from './paging'

export const PR_PAGE_SIZE = 50
// bounds re-walk cost on a run that dies far above every prior window, at the cost of
// re-fetching the dropped window's PRs once more before it gets picked back up
const MAX_COVERED_WINDOWS = 8

export type PrPageHandler = (prs: PullRequestNode[], sinceDate: Date | null) => Promise<void>

function isCovered(windows: CoveredWindow[], updatedAt: string): boolean {
  // updatedAt has second precision and is not unique — both edges stay exclusive so
  // PRs tied with a boundary timestamp are replayed instead of silently skipped
  const updated = new Date(updatedAt).getTime()
  return windows.some(
    (window) =>
      updated > new Date(window.confirmedThrough).getTime() &&
      updated < new Date(window.coveredUntil).getTime(),
  )
}

function mergeCoveredWindows(
  prior: CoveredWindow[],
  oldestSeen: string | null,
  newestSeen: string | null,
): CoveredWindow[] {
  if (!oldestSeen || !newestSeen) {
    return prior
  }

  let merged: CoveredWindow = { confirmedThrough: oldestSeen, coveredUntil: newestSeen }
  const untouched: CoveredWindow[] = []

  for (const window of prior) {
    // merge only once the walk passed strictly below this window's ceiling: dying above it
    // leaves an unwalked gap, dying exactly on it may split a timestamp tie — both stay separate
    if (new Date(oldestSeen).getTime() >= new Date(window.coveredUntil).getTime()) {
      untouched.push(window)
      continue
    }
    merged = {
      confirmedThrough:
        new Date(window.confirmedThrough).getTime() < new Date(merged.confirmedThrough).getTime()
          ? window.confirmedThrough
          : merged.confirmedThrough,
      coveredUntil:
        new Date(window.coveredUntil).getTime() > new Date(merged.coveredUntil).getTime()
          ? window.coveredUntil
          : merged.coveredUntil,
    }
  }

  const result = [merged, ...untouched].sort(
    (a, b) => new Date(b.coveredUntil).getTime() - new Date(a.coveredUntil).getTime(),
  )
  return result.slice(0, MAX_COVERED_WINDOWS)
}

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
    const data = await githubGraphql<PullRequestsPage>(
      ctx.http,
      PULL_REQUESTS_QUERY,
      {
        owner,
        repo,
        first: PR_PAGE_SIZE,
        cursor,
        direction: 'ASC',
      },
      ctx.log,
    )

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
  priorWindows: CoveredWindow[],
  processPrs: PrPageHandler,
): Promise<SyncOutcome> {
  const sinceDate = new Date(since)
  const runStartedAt = new Date().toISOString()
  // the walk is DESC by updatedAt, so a raw page cursor is a position that reorders
  // under it — never persist it across runs; resume coverage is value-based instead
  let cursor: string | null = null
  let oldestSeen: string | null = null
  let newestSeen: string | null = null

  const commitPartial = async () => {
    const coveredWindows = mergeCoveredWindows(priorWindows, oldestSeen, newestSeen)
    if (coveredWindows.length > 0) {
      await ctx.commitWatermark({ phase: 'incremental', since, cursor: null, coveredWindows })
    }
  }

  try {
    while (ctx.hasRunBudget()) {
      const data = await githubGraphql<PullRequestsPage>(
        ctx.http,
        PULL_REQUESTS_QUERY,
        {
          owner,
          repo,
          first: PR_PAGE_SIZE,
          cursor,
          direction: 'DESC',
        },
        ctx.log,
      )

      const { pageInfo, nodes } = data.repository.pullRequests
      const pullRequests = nodes.filter((node): node is PullRequestNode => node !== null)

      const fresh = pullRequests.filter((pr) => new Date(pr.updatedAt) >= sinceDate)
      const pending = fresh.filter((pr) => !isCovered(priorWindows, pr.updatedAt))
      if (pending.length > 0) {
        await processPrs(pending, sinceDate)
      }
      if (fresh.length > 0) {
        newestSeen = newestSeen ?? fresh[0].updatedAt
        oldestSeen = fresh[fresh.length - 1].updatedAt
      }

      const reachedSince = fresh.length < pullRequests.length
      if (reachedSince || !pageInfo.hasNextPage) {
        await ctx.commitWatermark({ phase: 'incremental', since: runStartedAt, cursor: null })
        return { complete: true }
      }

      cursor = pageInfo.endCursor
    }
  } catch (err) {
    await commitPartial()
    throw err
  }

  await commitPartial()
  return { complete: false }
}

export async function runDualPhasePrSync(
  ctx: SyncContext,
  processPrs: PrPageHandler,
): Promise<SyncOutcome> {
  const { owner, repo } = parseRepoChannel(ctx.channel.channelName)
  const watermark = readWatermark(ctx.watermark)

  if (watermark.phase === 'incremental' && watermark.since) {
    return runIncremental(ctx, owner, repo, watermark.since, watermark.coveredWindows, processPrs)
  }
  return runBackfill(ctx, owner, repo, processPrs)
}
