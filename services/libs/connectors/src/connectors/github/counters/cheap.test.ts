import { describe, expect, it, vi } from 'vitest'

import type { Logger } from '@crowd/logging'

import type { ConnectorHttp } from '../../../http/client'
import type { CountContext, SourceCounter } from '../../../types'
import { githubConnector } from '../index'
import { githubCheapCounters } from './cheap'

vi.mock('@crowd/integrations', () => ({
  GithubActivityType: {
    PULL_REQUEST_OPENED: 'pull_request-opened',
    PULL_REQUEST_MERGED: 'pull_request-merged',
    ISSUE_OPENED: 'issues-opened',
    ISSUE_CLOSED: 'issues-closed',
    DISCUSSION_STARTED: 'discussion-started',
    FORK: 'fork',
  },
  GITHUB_GRID: {},
}))

const log = { warn: vi.fn(), error: vi.fn(), info: vi.fn(), debug: vi.fn() } as unknown as Logger

const range = {
  kind: 'range',
  from: '2026-10-17T00:00:00Z',
  to: '2026-10-24T00:00:00Z',
} as const

interface CapturedRequest {
  data?: { query: string; variables: Record<string, unknown> }
  params?: { q: string; per_page: number }
}

function fakeContext(responses: unknown[]) {
  const requests: CapturedRequest[] = []
  let call = 0
  const http = {
    request: vi.fn(async (config: CapturedRequest) => {
      requests.push(config)
      return responses[Math.min(call++, responses.length - 1)]
    }),
    requestCount: () => call,
  } as unknown as ConnectorHttp
  const ctx = {
    channel: { channelName: 'https://github.com/foo/bar' },
    http,
    log,
  } as unknown as CountContext
  return { ctx, requests }
}

function counter(key: string): SourceCounter {
  return githubCheapCounters.find((c) => c.key === key) as SourceCounter
}

describe('github cheap counters', () => {
  it('counts a range through inclusive search qualifiers', async () => {
    const { ctx, requests } = fakeContext([{ total_count: 7 }])
    const window = '2026-10-17T00:00:00Z..2026-10-23T23:59:59Z'

    for (const [key, qualifiers] of [
      ['pull_request-opened', `is:pr created:${window}`],
      ['pull_request-merged', `is:pr is:merged merged:${window}`],
      ['issues-opened', `is:issue created:${window}`],
      ['issues-closed', `is:issue is:closed closed:${window}`],
    ]) {
      expect(await counter(key).count(ctx, range)).toBe(7)
      expect(requests[requests.length - 1].params).toEqual({
        q: `repo:foo/bar ${qualifiers}`,
        per_page: 1,
      })
    }
  })

  it('counts discussions in a range through GraphQL search', async () => {
    const { ctx, requests } = fakeContext([{ data: { search: { discussionCount: 3 } } }])

    expect(await counter('discussion-started').count(ctx, range)).toBe(3)
    expect(requests[0].data?.variables).toEqual({
      q: 'repo:foo/bar created:2026-10-17T00:00:00Z..2026-10-23T23:59:59Z',
    })
  })

  it('reads each all-time total from its own repository field', async () => {
    const cases = [
      ['pull_request-opened', 'pullRequests'],
      ['pull_request-merged', 'merged'],
      ['issues-opened', 'issues'],
      ['issues-closed', 'closed'],
      ['discussion-started', 'discussions'],
      ['fork', 'forks'],
    ]
    for (const [key, field] of cases) {
      const { ctx, requests } = fakeContext([
        { data: { repository: { [field]: { totalCount: 42 } } } },
      ])
      expect(await counter(key).count(ctx, { kind: 'all-time' })).toBe(42)
      expect(requests[0].data?.query).toContain(`${field}:`)
    }
  })

  it('counts forks in a range, excluding the end and stopping at the first older fork', async () => {
    const page = (createdAts: string[], hasNextPage: boolean) => ({
      data: {
        repository: {
          forks: {
            pageInfo: { endCursor: 'c', hasNextPage },
            nodes: createdAts.map((createdAt) => ({ createdAt })),
          },
        },
      },
    })
    const { ctx, requests } = fakeContext([
      page(['2026-10-24T00:00:00Z', '2026-10-23T12:00:00Z'], true),
      page(['2026-10-17T00:00:00Z', '2026-10-16T23:59:59Z'], true),
      page(['2026-10-10T00:00:00Z'], false),
    ])

    expect(await counter('fork').count(ctx, range)).toBe(2)
    expect(requests).toHaveLength(2)
  })

  it('reports backfill complete only in the incremental phase', () => {
    const isComplete = githubConnector.isBackfillComplete
    expect(isComplete?.({ phase: 'incremental' })).toBe(true)
    expect(isComplete?.({ phase: 'backfill' })).toBe(false)
    expect(isComplete?.(null)).toBe(false)
  })
})
