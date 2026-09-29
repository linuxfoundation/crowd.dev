import { describe, expect, it, vi } from 'vitest'

import type { Logger } from '@crowd/logging'

import type { ConnectorHttp } from '../../../http/client'
import { ProviderUnavailableError } from '../../../http/errors'
import type { SyncContext } from '../../../types'
import { PR_COMMITS_QUERY } from '../graphql/pullRequestChildren'
import type { GithubActivity } from '../schemas'
import { pullRequestCommitsSync } from './pullRequestCommits'

// @crowd/integrations eagerly scans and requires every integration folder on import,
// which fails outside its build environment and is unrelated to what's under test here.
vi.mock('@crowd/integrations', () => ({
  GithubActivityType: { AUTHORED_COMMIT: 'authored-commit' },
  GITHUB_GRID: { 'authored-commit': { score: 1 } },
}))

const CHANNEL = { channelId: 'c1', channelName: 'https://github.com/openclaw/openclaw' }

interface RequestLog {
  prNumber: number
  usedStatsQuery: boolean
  cursor: string | null
}

interface HarnessOptions {
  prNumbers: number[]
  // PR numbers whose stats query should always fail, forcing a no-stats fallback
  statsUnavailableForPrs?: number[]
  // number of commit pages to serve for a given PR (default 1)
  pagesPerPr?: Record<number, number>
  // PR numbers whose commits have no resolvable GitHub user (author.user = null)
  ghostAuthorPrs?: number[]
  reportedTotalCount?: Record<number, number>
  nullNodePrs?: number[]
  statsDroppedNodePrs?: number[]
  missingPullRequestPrs?: number[]
}

function commitNode(id: string, ghostAuthor = false) {
  return {
    commit: {
      parents: { totalCount: 1 },
      id,
      oid: id,
      message: 'msg',
      authoredDate: '2026-09-20T00:00:00Z',
      url: `https://github.com/openclaw/openclaw/commit/${id}`,
      author: ghostAuthor
        ? { user: null, email: 'someone@example.com', name: 'Someone' }
        : { user: { login: 'someone' }, email: null, name: null },
    },
  }
}

function makeHarness(opts: HarnessOptions) {
  const requests: RequestLog[] = []
  const emitted: GithubActivity[] = []
  let prListServed = false
  const warn = vi.fn()

  const http = {
    request: async (config: { data: { query: string; variables: Record<string, unknown> } }) => {
      const { query, variables } = config.data

      if (query.includes('pullRequests(')) {
        if (prListServed) {
          return {
            data: {
              repository: {
                pullRequests: { pageInfo: { hasNextPage: false, endCursor: null }, nodes: [] },
              },
            },
          }
        }
        prListServed = true
        return {
          data: {
            repository: {
              pullRequests: {
                pageInfo: { hasNextPage: false, endCursor: null },
                nodes: opts.prNumbers.map((number) => ({
                  id: `pr-${number}`,
                  number,
                  updatedAt: '2026-09-20T00:00:00Z',
                })),
              },
            },
          },
        }
      }

      const { prNumber, cursor } = variables as { prNumber: number; cursor: string | null }
      const usedStatsQuery = query === PR_COMMITS_QUERY
      requests.push({ prNumber, usedStatsQuery, cursor })

      if (usedStatsQuery && opts.statsUnavailableForPrs?.includes(prNumber)) {
        throw new ProviderUnavailableError()
      }

      const pageIndex = cursor ? Number(cursor.slice(1)) : 1
      const totalPages = opts.pagesPerPr?.[prNumber] ?? 1
      const hasNextPage = pageIndex < totalPages
      if (opts.missingPullRequestPrs?.includes(prNumber)) {
        return { data: { repository: { pullRequest: null } } }
      }
      const reportedTotalCount = opts.reportedTotalCount?.[prNumber]
      const secondNode = opts.statsDroppedNodePrs?.includes(prNumber)
        ? usedStatsQuery
          ? null
          : commitNode(`${prNumber}-${pageIndex}-b`)
        : opts.nullNodePrs?.includes(prNumber)
          ? null
          : undefined
      return {
        data: {
          repository: {
            pullRequest: {
              commits: {
                ...(reportedTotalCount === undefined ? {} : { totalCount: reportedTotalCount }),
                pageInfo: {
                  endCursor: hasNextPage ? `p${pageIndex + 1}` : null,
                  hasNextPage,
                },
                nodes: [
                  commitNode(`${prNumber}-${pageIndex}`, opts.ghostAuthorPrs?.includes(prNumber)),
                  ...(secondNode === undefined ? [] : [secondNode]),
                ],
              },
            },
          },
        },
      }
    },
  } as unknown as ConnectorHttp

  const ctx: SyncContext = {
    channel: CHANNEL,
    watermark: { phase: 'backfill', since: null, cursor: null },
    emit: async (records) => {
      emitted.push(...(records as GithubActivity[]))
    },
    commitWatermark: async () => {},
    hasRunBudget: () => true,
    http,
    log: {
      child: () => ctx.log,
      info: () => {},
      warn,
      error: () => {},
    } as unknown as Logger,
  }

  return { ctx, requests, emitted, warn }
}

describe('pullRequestCommitsSync', () => {
  it('uses the stats query for a PR whose commits fetch normally', async () => {
    const { ctx, requests } = makeHarness({ prNumbers: [1] })

    await pullRequestCommitsSync.run(ctx)

    expect(requests).toEqual([{ prNumber: 1, usedStatsQuery: true, cursor: null }])
  })

  it('falls back to the no-stats query once, then stays on it for the rest of that PR', async () => {
    const { ctx, requests } = makeHarness({
      prNumbers: [2],
      statsUnavailableForPrs: [2],
      pagesPerPr: { 2: 2 },
    })

    await pullRequestCommitsSync.run(ctx)

    // page 1: stats attempted and failed, then no-stats fallback
    expect(requests[0]).toEqual({ prNumber: 2, usedStatsQuery: true, cursor: null })
    expect(requests[1]).toEqual({ prNumber: 2, usedStatsQuery: false, cursor: null })
    // page 2 of the same PR: stats is skipped entirely, straight to no-stats
    expect(requests[2]).toEqual({ prNumber: 2, usedStatsQuery: false, cursor: 'p2' })
    expect(requests).toHaveLength(3)
  })

  it('emits commits whose author has no github account as the ghost member', async () => {
    const { ctx, emitted } = makeHarness({ prNumbers: [5], ghostAuthorPrs: [5] })

    await pullRequestCommitsSync.run(ctx)

    expect(emitted).toHaveLength(1)
    expect(emitted[0].sourceId).toBe('5-1')
    expect(emitted[0].member.displayName).toBe('ghost')
    expect(emitted[0].member.identities[0].value).toBe('ghost')
  })

  it('does not carry the no-stats fallback over to the next PR', async () => {
    const { ctx, requests } = makeHarness({ prNumbers: [3, 4], statsUnavailableForPrs: [3] })

    await pullRequestCommitsSync.run(ctx)

    const pr3 = requests.filter((r) => r.prNumber === 3).map((r) => r.usedStatsQuery)
    const pr4 = requests.filter((r) => r.prNumber === 4).map((r) => r.usedStatsQuery)
    expect(pr3).toEqual([true, false])
    expect(pr4).toEqual([true])
  })

  it('warns when github reports more commits than were fetched for a pull request', async () => {
    const { ctx, emitted, warn } = makeHarness({
      prNumbers: [6],
      reportedTotalCount: { 6: 3 },
      nullNodePrs: [6],
    })

    await pullRequestCommitsSync.run(ctx)

    expect(emitted).toHaveLength(1)
    expect(warn).toHaveBeenCalledWith(
      { prNumber: 6, expectedCount: 3, fetchedCount: 1 },
      'fetched fewer commits than github reports for pull request',
    )
  })

  it('does not warn when every reported commit was fetched', async () => {
    const { ctx, warn } = makeHarness({
      prNumbers: [7],
      reportedTotalCount: { 7: 2 },
      pagesPerPr: { 7: 2 },
    })

    await pullRequestCommitsSync.run(ctx)

    expect(warn).not.toHaveBeenCalled()
  })

  it('refetches a page without stats when the stats response drops a commit node', async () => {
    const { ctx, requests, emitted, warn } = makeHarness({
      prNumbers: [9],
      reportedTotalCount: { 9: 2 },
      statsDroppedNodePrs: [9],
    })

    await pullRequestCommitsSync.run(ctx)

    expect(requests).toEqual([
      { prNumber: 9, usedStatsQuery: true, cursor: null },
      { prNumber: 9, usedStatsQuery: false, cursor: null },
    ])
    expect(emitted.map((record) => record.sourceId)).toEqual(['9-1', '9-1-b'])
    expect(warn).toHaveBeenCalledTimes(1)
    expect(warn).toHaveBeenCalledWith(
      'github dropped commits from a diff stats page, refetching without them',
    )
  })

  it('warns when github returns no pull request for a listed one', async () => {
    const { ctx, emitted, warn } = makeHarness({ prNumbers: [8], missingPullRequestPrs: [8] })

    await pullRequestCommitsSync.run(ctx)

    expect(emitted).toHaveLength(0)
    expect(warn).toHaveBeenCalledWith('github returned no commits connection for pull request')
  })
})
