import { describe, expect, it, vi } from 'vitest'

import type { Logger } from '@crowd/logging'

import type { ConnectorHttp } from '../../../http/client'
import type { CountContext, SourceCounter } from '../../../types'
import { selectCounters } from '../../../validation'
import { githubConnector } from '../index'
import { githubPrTimelineCounters } from './prTimeline'

vi.mock('@crowd/integrations', () => ({
  GithubActivityType: {
    PULL_REQUEST_OPENED: 'pull_request-opened',
    PULL_REQUEST_MERGED: 'pull_request-merged',
    PULL_REQUEST_CLOSED: 'pull_request-closed',
    PULL_REQUEST_REVIEWED: 'pull_request-reviewed',
    PULL_REQUEST_REVIEW_REQUESTED: 'pull_request-review-requested',
    PULL_REQUEST_ASSIGNED: 'pull_request-assigned',
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
  return githubPrTimelineCounters.find((c) => c.key === key) as SourceCounter
}

function page(nodes: unknown[], hasNextPage: boolean) {
  return {
    data: { repository: { pullRequests: { pageInfo: { endCursor: 'c', hasNextPage }, nodes } } },
  }
}

const count = (filteredCount: number) => ({ filteredCount })

const EVENTS = ['closed_event', 'pull_request_review', 'review_requested_event', 'assigned_event']

function pr(bounds: string[], counts: Record<string, number>, updatedAt?: string) {
  return {
    ...(updatedAt ? { updatedAt } : {}),
    ...Object.fromEntries(
      EVENTS.flatMap((event) =>
        bounds.map((bound) => [`${event}_${bound}`, count(counts[`${event}_${bound}`] ?? 0)]),
      ),
    ),
  }
}

describe('github pr timeline counters', () => {
  it('sums all-time filteredCount across pages and fails on a null pull request node', async () => {
    const { ctx, requests } = fakeContext([
      page([pr(['all'], { assigned_event_all: 2 })], true),
      page([pr(['all'], { assigned_event_all: 3 })], false),
    ])

    expect(await counter('pull_request-assigned').count(ctx, { kind: 'all-time' })).toBe(5)
    expect(requests).toHaveLength(2)
    expect(requests[0].data?.query).toContain('itemTypes: [ASSIGNED_EVENT]')

    const nulled = fakeContext([page([pr(['all'], { assigned_event_all: 2 }), null], false)])
    await expect(
      counter('pull_request-assigned').count(nulled.ctx, { kind: 'all-time' }),
    ).rejects.toThrow('null pull request node')
  })

  it('counts a range as since-from minus since-to, keeps lagged PRs, and stops below the lag floor', async () => {
    const { ctx, requests } = fakeContext([
      page(
        [
          pr(['from', 'to'], { closed_event_from: 5, closed_event_to: 1 }, '2026-10-25T00:00:00Z'),
          pr(['from', 'to'], { closed_event_from: 2 }, '2026-10-18T00:00:00Z'),
          pr(['from', 'to'], { closed_event_from: 9 }, '2026-10-16T00:00:00Z'),
          pr(['from', 'to'], { closed_event_from: 3 }, '2026-10-20T00:00:00Z'),
        ],
        true,
      ),
      page([pr(['from', 'to'], { closed_event_from: 7 }, '2026-10-10T00:00:00Z')], true),
    ])

    expect(await counter('pull_request-closed').count(ctx, range)).toBe(9)
    expect(requests).toHaveLength(2)
    expect(requests[0].data?.variables).toMatchObject({ from: range.from, to: range.to })
  })

  it('declares key, sync and cost for each counter', () => {
    expect(githubPrTimelineCounters.map((c) => [c.key, c.syncName, c.activityTypes])).toEqual(
      [
        'pull_request-closed',
        'pull_request-reviewed',
        'pull_request-review-requested',
        'pull_request-assigned',
      ].map((key) => [key, 'pull-requests', [key]]),
    )
    for (const c of githubPrTimelineCounters) {
      expect(c.cost).toEqual({ range: 'cheap', 'all-time': 'expensive' })
    }
  })

  it('is selected by default for ranges but not for all-time', () => {
    const keys = (kind: 'range' | 'all-time') =>
      selectCounters(githubConnector, 'pull-requests', kind, null).map((c) => c.key)

    expect(keys('range')).toEqual(
      expect.arrayContaining(githubPrTimelineCounters.map((c) => c.key)),
    )
    expect(keys('all-time')).not.toContain('pull_request-closed')
  })
})
