import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { Logger } from '@crowd/logging'

import { RateLimitError } from '../../http/errors'
import { fetchFeeds, fetchKeywords, fetchNotifications } from './configClient'
import type { OctolensFeed, OctolensKeyword, OctolensNotification } from './configClient'
import { runMappingDiscovery } from './mappingDiscovery'
import type { OctolensKeywordMappingInput, OctolensMappingDiscoveryInput } from './mappingDiscovery'

vi.mock('./configClient', () => ({
  fetchNotifications: vi.fn(),
  fetchFeeds: vi.fn(),
  fetchKeywords: vi.fn(),
}))

const mockedNotifications = vi.mocked(fetchNotifications)
const mockedFeeds = vi.mocked(fetchFeeds)
const mockedKeywords = vi.mocked(fetchKeywords)

const SEGMENTS: Record<string, string> = { ebpf: 'segment-ebpf', valkey: 'segment-valkey' }

function webhook(slug: string) {
  return {
    type: 'WEBHOOK',
    webhookDestination: {
      url: `https://insights.linuxfoundation.org/api/community/${slug}?auth=x`,
    },
  }
}

function notification(overrides: Partial<OctolensNotification> = {}): OctolensNotification {
  return {
    id: 1,
    name: 'eBPF',
    enabled: true,
    feedId: 10,
    destinations: [webhook('ebpf')],
    ...overrides,
  }
}

function keywordFeed(id: number, keywordIds: string): OctolensFeed {
  return {
    id,
    name: `feed-${id}`,
    simpleFilters: { conditions: [{ field: 'Keywords', values: keywordIds }] },
    advancedFilters: null,
  }
}

function keyword(id: number, paused = false): OctolensKeyword {
  return { id, keyword: `keyword-${id}`, paused }
}

function makeCtx() {
  const upserts: OctolensKeywordMappingInput[] = []
  const deactivateCalls: number[][] = []
  const log = { info: vi.fn(), warn: vi.fn(), error: vi.fn() }
  const ctx: OctolensMappingDiscoveryInput = {
    token: 'test-token',
    log: log as unknown as Logger,
    resolveProjectSegmentId: vi.fn(async (slug: string) => SEGMENTS[slug] ?? null),
    reconcileMappings: async (mappings) => {
      upserts.push(...mappings)
      deactivateCalls.push(mappings.map((m) => m.keywordId))
      return 0
    },
  }
  return { ctx, deps: ctx, upserts, deactivateCalls, log }
}

function givenOctolens(
  notifications: OctolensNotification[],
  feeds: OctolensFeed[],
  keywords: OctolensKeyword[],
) {
  mockedNotifications.mockResolvedValue(notifications)
  mockedFeeds.mockResolvedValue(feeds)
  mockedKeywords.mockResolvedValue(keywords)
}

beforeEach(() => {
  mockedNotifications.mockReset()
  mockedFeeds.mockReset()
  mockedKeywords.mockReset()
})

describe('runMappingDiscovery', () => {
  it('maps every keyword of a webhook feed to the slug project segment, then reconciles', async () => {
    givenOctolens(
      [notification(), notification({ id: 2, feedId: 20, destinations: [webhook('valkey')] })],
      [keywordFeed(10, '1,2'), keywordFeed(20, '3')],
      [keyword(1), keyword(2), keyword(3)],
    )
    const { ctx, upserts, deactivateCalls } = makeCtx()

    const outcome = await runMappingDiscovery(ctx)

    expect(outcome).toEqual({ complete: true, mappings: 3, deactivated: 0 })
    expect(upserts).toEqual([
      { keywordId: 1, keyword: 'keyword-1', segmentId: 'segment-ebpf' },
      { keywordId: 2, keyword: 'keyword-2', segmentId: 'segment-ebpf' },
      { keywordId: 3, keyword: 'keyword-3', segmentId: 'segment-valkey' },
    ])
    expect(deactivateCalls).toEqual([[1, 2, 3]])
  })

  it('ignores paused notifications, so their mappings get deactivated', async () => {
    givenOctolens(
      [notification(), notification({ id: 2, feedId: 20, enabled: false })],
      [keywordFeed(10, '1'), keywordFeed(20, '2')],
      [keyword(1), keyword(2)],
    )
    const { ctx, upserts, deactivateCalls } = makeCtx()

    await runMappingDiscovery(ctx)

    expect(upserts.map((u) => u.keywordId)).toEqual([1])
    expect(deactivateCalls).toEqual([[1]])
  })

  it('ignores notifications without an Insights webhook', async () => {
    givenOctolens(
      [
        notification(),
        notification({ id: 2, feedId: 20, destinations: [{ type: 'SLACK' }] }),
        notification({
          id: 3,
          feedId: 20,
          destinations: [{ type: 'WEBHOOK', webhookDestination: { url: 'https://example.com/x' } }],
        }),
      ],
      [keywordFeed(10, '1'), keywordFeed(20, '2')],
      [keyword(1), keyword(2)],
    )
    const { ctx, upserts } = makeCtx()

    await runMappingDiscovery(ctx)

    expect(upserts.map((u) => u.keywordId)).toEqual([1])
  })

  it('skips and logs a feed with no Keywords condition instead of mapping every keyword', async () => {
    givenOctolens(
      [notification(), notification({ id: 2, feedId: 20, destinations: [webhook('valkey')] })],
      [
        keywordFeed(10, '1'),
        { id: 20, name: 'Zephyr', simpleFilters: null, advancedFilters: null },
      ],
      [keyword(1), keyword(2)],
    )
    const { ctx, upserts, log } = makeCtx()

    await runMappingDiscovery(ctx)

    expect(upserts.map((u) => u.keywordId)).toEqual([1])
    expect(log.warn).toHaveBeenCalledWith(
      expect.objectContaining({ feedId: 20, reason: 'no keyword condition' }),
      'octolens mapping discovery: skipping feed',
    )
  })

  it('does not map a keyword that reaches several projects', async () => {
    givenOctolens(
      [notification(), notification({ id: 2, feedId: 20, destinations: [webhook('valkey')] })],
      [keywordFeed(10, '1,2'), keywordFeed(20, '2')],
      [keyword(1), keyword(2)],
    )
    const { ctx, upserts, deactivateCalls } = makeCtx()

    await runMappingDiscovery(ctx)

    expect(upserts.map((u) => u.keywordId)).toEqual([1])
    expect(deactivateCalls).toEqual([[1]])
  })

  it('maps a keyword reached by several notifications of the same project once', async () => {
    givenOctolens(
      [notification(), notification({ id: 2, feedId: 20 })],
      [keywordFeed(10, '1'), keywordFeed(20, '1')],
      [keyword(1)],
    )
    const { ctx, upserts } = makeCtx()

    await runMappingDiscovery(ctx)

    expect(upserts).toEqual([{ keywordId: 1, keyword: 'keyword-1', segmentId: 'segment-ebpf' }])
  })

  it('skips a slug that does not resolve to a project segment, looking it up once', async () => {
    givenOctolens(
      [notification({ destinations: [webhook('unknown')] })],
      [keywordFeed(10, '1,2')],
      [keyword(1), keyword(2)],
    )
    const { ctx, deps, upserts } = makeCtx()

    await runMappingDiscovery(ctx)

    expect(upserts).toEqual([])
    expect(deps.resolveProjectSegmentId).toHaveBeenCalledTimes(1)
  })

  it('skips a keyword id the keyword list does not know', async () => {
    givenOctolens([notification()], [keywordFeed(10, '1,99')], [keyword(1)])
    const { ctx, upserts } = makeCtx()

    await runMappingDiscovery(ctx)

    expect(upserts.map((u) => u.keywordId)).toEqual([1])
  })

  it('logs keywords no project webhook reaches without writing them', async () => {
    givenOctolens([notification()], [keywordFeed(10, '1')], [keyword(1), keyword(2, true)])
    const { ctx, upserts, log } = makeCtx()

    await runMappingDiscovery(ctx)

    expect(upserts.map((u) => u.keywordId)).toEqual([1])
    expect(log.info).toHaveBeenCalledWith(
      { unreachedKeywords: [{ id: 2, keyword: 'keyword-2', paused: true }] },
      'octolens mapping discovery: keywords no project webhook reaches',
    )
  })

  it('writes nothing when a notification points at a missing feed', async () => {
    givenOctolens(
      [notification(), notification({ id: 2, feedId: 404, destinations: [webhook('valkey')] })],
      [keywordFeed(10, '1')],
      [keyword(1)],
    )
    const { ctx, upserts, deactivateCalls } = makeCtx()

    await runMappingDiscovery(ctx)

    expect(upserts).toEqual([])
    expect(deactivateCalls).toEqual([])
  })

  it('writes nothing when a feed has malformed keyword ids', async () => {
    givenOctolens(
      [notification(), notification({ id: 2, feedId: 20, destinations: [webhook('valkey')] })],
      [keywordFeed(10, '1'), keywordFeed(20, '')],
      [keyword(1)],
    )
    const { ctx, upserts, deactivateCalls } = makeCtx()

    await runMappingDiscovery(ctx)

    expect(upserts).toEqual([])
    expect(deactivateCalls).toEqual([])
  })

  it('deactivates nothing when the run finds no mappings', async () => {
    givenOctolens([], [], [keyword(1)])
    const { ctx, deactivateCalls } = makeCtx()

    await runMappingDiscovery(ctx)

    expect(deactivateCalls).toEqual([])
  })

  it('writes and deactivates nothing when an Octolens fetch fails', async () => {
    mockedNotifications.mockResolvedValue([notification()])
    mockedFeeds.mockRejectedValue(new RateLimitError('rate limited', { status: 429 }))
    mockedKeywords.mockResolvedValue([keyword(1)])
    const { ctx, upserts, deactivateCalls } = makeCtx()

    await expect(runMappingDiscovery(ctx)).rejects.toBeInstanceOf(RateLimitError)
    expect(upserts).toEqual([])
    expect(deactivateCalls).toEqual([])
  })
})
