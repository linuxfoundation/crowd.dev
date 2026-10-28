import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type { Logger } from '@crowd/logging'
import { PlatformType } from '@crowd/types'

import type { Credential, SyncContext } from '../../../types'
import type { OctolensDeps } from '../deps'
import { fetchMentionsPage } from '../mentionsClient'
import type { OctolensMention, OctolensMentionsPage } from '../mentionsClient'
import { runMentionPoll } from './mentionPoll'

vi.mock('../mentionsClient', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../mentionsClient')>()
  return { ...actual, fetchMentionsPage: vi.fn() }
})

const mockedFetchMentionsPage = vi.mocked(fetchMentionsPage)

type OctolensSyncContext = SyncContext<OctolensDeps>

const OCTOLENS_CREDENTIAL: Credential = {
  platform: 'octolens',
  kind: 'octolens-token',
  data: { token: 'test-token' },
}

const LOG = { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() } as unknown as Logger

// Like the live API: the keyword lives only in keywords[], with no top-level keywordId.
function mention(overrides: Partial<OctolensMention> = {}): OctolensMention {
  const { keywordId = 1, ...rest } = overrides
  return {
    id: 1,
    sourceId: 'src-1',
    url: 'https://example.com',
    title: null,
    body: null,
    source: 'github',
    timestamp: '2026-10-12 08:00:00.000',
    author: 'octocat',
    authorName: 'The Octocat',
    authorUrl: 'https://github.com/octocat',
    authorAvatar: null,
    authorFollowers: null,
    relevance: 'relevant',
    relevanceComment: null,
    sentiment: null,
    language: null,
    tags: [],
    keywords: keywordId === undefined ? [] : [{ id: keywordId, keyword: `kw-${keywordId}` }],
    engaged: false,
    isReply: false,
    ...rest,
  }
}

function target(segmentId: string) {
  return { segmentId, integrationId: `integration-for-${segmentId}` }
}

function page(data: OctolensMention[], nextCursor: string | null = null): OctolensMentionsPage {
  return { data, nextCursor }
}

function makeCtx(
  overrides: Partial<Omit<OctolensSyncContext, 'deps'>> & Partial<OctolensDeps> = {},
): {
  ctx: OctolensSyncContext
  emitted: { records: unknown[]; segmentId?: string; integrationId?: string }[]
  watermarks: Record<string, unknown>[]
} {
  const emitted: { records: unknown[]; segmentId?: string; integrationId?: string }[] = []
  const watermarks: Record<string, unknown>[] = []

  const { resolveKeywordTarget, ...ctxOverrides } = overrides
  const ctx: OctolensSyncContext = {
    channel: { channelId: 'octolens-global', channelName: 'octolens-global' },
    integrationId: 'integration-1',
    watermark: null,
    emit: async (records, emitOverrides) => {
      emitted.push({
        records,
        segmentId: emitOverrides?.segmentId,
        integrationId: emitOverrides?.integrationId,
      })
    },
    commitWatermark: async (watermark) => {
      watermarks.push(watermark)
    },
    hasRunBudget: () => true,
    http: {} as OctolensSyncContext['http'],
    log: LOG,
    getCredential: async () => OCTOLENS_CREDENTIAL,
    deps: {
      resolveKeywordTarget:
        resolveKeywordTarget ?? (async (keywordId) => target(`segment-${keywordId}`)),
    },
    ...ctxOverrides,
  }

  return { ctx, emitted, watermarks }
}

function pollFields({ startDate, cursor, pendingMaxTimestamp }: Record<string, unknown>) {
  return { startDate, cursor, pendingMaxTimestamp }
}

// Before every fixture timestamp, so a first run's "now" start date is overtaken by the mentions.
const NOW = '2026-10-01T00:00:00.000Z'
const INITIAL_CUTOFF = { startDate: NOW, cursor: null, pendingMaxTimestamp: null }

beforeEach(() => {
  mockedFetchMentionsPage.mockReset()
  vi.useFakeTimers({ now: new Date(NOW), toFake: ['Date'] })
})

afterEach(() => {
  vi.useRealTimers()
})

describe('runMentionPoll', () => {
  it('emits mapped mentions grouped by subproject integration and commits the watermark', async () => {
    mockedFetchMentionsPage.mockResolvedValue(
      page([mention({ keywordId: 1 }), mention({ keywordId: 2, sourceId: 'src-2' })]),
    )
    const { ctx, emitted, watermarks } = makeCtx()

    const outcome = await runMentionPoll(ctx)

    expect(outcome).toEqual({ complete: true })
    expect(emitted).toHaveLength(2)
    expect(emitted.map(({ segmentId, integrationId }) => ({ segmentId, integrationId }))).toEqual(
      expect.arrayContaining([
        { segmentId: 'segment-1', integrationId: 'integration-for-segment-1' },
        { segmentId: 'segment-2', integrationId: 'integration-for-segment-2' },
      ]),
    )
    expect(watermarks.map(pollFields)).toEqual([
      INITIAL_CUTOFF,
      { startDate: '2026-10-12T08:00:00.000Z', cursor: null, pendingMaxTimestamp: null },
    ])
  })

  it('groups multiple mentions mapping to the same segment into one emit call', async () => {
    mockedFetchMentionsPage.mockResolvedValue(
      page([mention({ keywordId: 1 }), mention({ keywordId: 1, sourceId: 'src-2' })]),
    )
    const { ctx, emitted } = makeCtx()

    await runMentionPoll(ctx)

    expect(emitted).toHaveLength(1)
    expect(emitted[0].records).toHaveLength(2)
  })

  it('skips a mention with an unparseable timestamp instead of throwing', async () => {
    mockedFetchMentionsPage.mockResolvedValue(
      page([
        mention({ timestamp: 'not-a-timestamp', sourceId: 'bad' }),
        mention({ sourceId: 'ok' }),
      ]),
    )
    const { ctx, emitted, watermarks } = makeCtx()

    const outcome = await runMentionPoll(ctx)

    expect(outcome).toEqual({ complete: true })
    expect(emitted).toHaveLength(1)
    expect((emitted[0].records[0] as { attributes: { postId: string } }).attributes.postId).toBe(
      'ok',
    )
    expect(watermarks.map(pollFields)).toEqual([
      INITIAL_CUTOFF,
      { startDate: '2026-10-12T08:00:00.000Z', cursor: null, pendingMaxTimestamp: null },
    ])
  })

  it('skips an unsupported source without throwing or emitting it', async () => {
    mockedFetchMentionsPage.mockResolvedValue(
      page([mention({ source: 'podcasts', authorUrl: 'https://www.spreaker.com/show/x' })]),
    )
    const { ctx, emitted } = makeCtx()

    const outcome = await runMentionPoll(ctx)

    expect(outcome).toEqual({ complete: true })
    expect(emitted).toHaveLength(0)
  })

  it('skips a mention Octolens marks not relevant and keeps the rest of the page', async () => {
    mockedFetchMentionsPage.mockResolvedValue(
      page([mention({ relevance: 'not_relevant' }), mention({ id: 2, sourceId: 's2' })]),
    )
    const { ctx, emitted } = makeCtx()

    await runMentionPoll(ctx)

    expect(emitted).toHaveLength(1)
    expect(emitted[0].records.map((r) => (r as { sourceId: string }).sourceId)).toEqual(['2'])
  })

  it('skips a mention whose keywordId has no mapping', async () => {
    mockedFetchMentionsPage.mockResolvedValue(page([mention()]))
    const { ctx, emitted } = makeCtx({ resolveKeywordTarget: async () => null })

    await runMentionPoll(ctx)

    expect(emitted).toHaveLength(0)
  })

  it('skips a mention with no keywords without calling resolveKeywordTarget', async () => {
    mockedFetchMentionsPage.mockResolvedValue(page([mention({ keywords: [] })]))
    const resolveKeywordTarget = vi.fn(async () => target('segment-1'))
    const { ctx, emitted } = makeCtx({ resolveKeywordTarget })

    await runMentionPoll(ctx)

    expect(emitted).toHaveLength(0)
    expect(resolveKeywordTarget).not.toHaveBeenCalled()
  })

  it('skips a mention that fails to map (e.g. a malformed field) instead of crashing the page', async () => {
    mockedFetchMentionsPage.mockResolvedValue(
      page([
        {
          ...mention({ sourceId: 'bad' }),
          source: undefined,
          authorUrl: undefined,
        } as unknown as OctolensMention,
        mention({ sourceId: 'ok' }),
      ]),
    )
    const { ctx, emitted } = makeCtx()

    const outcome = await runMentionPoll(ctx)

    expect(outcome).toEqual({ complete: true })
    expect(emitted).toHaveLength(1)
    expect((emitted[0].records[0] as { attributes: { postId: string } }).attributes.postId).toBe(
      'ok',
    )
  })

  it('still advances the watermark past a mention whose timestamp parsed but mapping failed', async () => {
    mockedFetchMentionsPage.mockResolvedValue(
      page([
        {
          ...mention({ timestamp: '2026-10-12 09:00:00.000' }),
          source: undefined,
          authorUrl: undefined,
        } as unknown as OctolensMention,
      ]),
    )
    const { ctx, emitted, watermarks } = makeCtx()

    await runMentionPoll(ctx)

    expect(emitted).toHaveLength(0)
    expect(watermarks.map(pollFields)).toEqual([
      INITIAL_CUTOFF,
      { startDate: '2026-10-12T09:00:00.000Z', cursor: null, pendingMaxTimestamp: null },
    ])
  })

  it('skips a mention whose mapped activity fails schema validation instead of emitting it', async () => {
    mockedFetchMentionsPage.mockResolvedValue(
      page([
        { ...mention({ sourceId: 'bad-title' }), title: 123 } as unknown as OctolensMention,
        mention({ sourceId: 'ok' }),
      ]),
    )
    const { ctx, emitted } = makeCtx()

    const outcome = await runMentionPoll(ctx)

    expect(outcome).toEqual({ complete: true })
    expect(emitted).toHaveLength(1)
    expect((emitted[0].records[0] as { attributes: { postId: string } }).attributes.postId).toBe(
      'ok',
    )
  })

  it('skips a null page entry instead of crashing the whole page', async () => {
    mockedFetchMentionsPage.mockResolvedValue(
      page([null as unknown as OctolensMention, mention({ sourceId: 'ok' })]),
    )
    const { ctx, emitted } = makeCtx()

    const outcome = await runMentionPoll(ctx)

    expect(outcome).toEqual({ complete: true })
    expect(emitted).toHaveLength(1)
    expect((emitted[0].records[0] as { attributes: { postId: string } }).attributes.postId).toBe(
      'ok',
    )
  })

  it('propagates a resolveKeywordTarget failure instead of swallowing it as a skip', async () => {
    mockedFetchMentionsPage.mockResolvedValue(page([mention()]))
    const { ctx, watermarks } = makeCtx({
      resolveKeywordTarget: async () => {
        throw new Error('db connection lost')
      },
    })

    await expect(runMentionPoll(ctx)).rejects.toThrow('db connection lost')
    // Only the first run's cutoff: a transient failure must not advance past this mention.
    expect(watermarks.map(pollFields)).toEqual([INITIAL_CUTOFF])
  })

  it('still advances the watermark past a skipped mention', async () => {
    mockedFetchMentionsPage.mockResolvedValue(
      page([mention({ source: 'podcasts', authorUrl: 'https://www.spreaker.com/show/x' })]),
    )
    const { ctx, emitted, watermarks } = makeCtx()

    await runMentionPoll(ctx)

    expect(emitted).toHaveLength(0)
    expect(watermarks.map(pollFields)).toEqual([
      INITIAL_CUTOFF,
      { startDate: '2026-10-12T08:00:00.000Z', cursor: null, pendingMaxTimestamp: null },
    ])
  })

  it('paginates until nextCursor is null, passing the cursor and the original startDate through', async () => {
    mockedFetchMentionsPage
      .mockResolvedValueOnce(page([mention({ sourceId: 'page-1' })], 'cursor-2'))
      .mockResolvedValueOnce(
        page([mention({ sourceId: 'page-2', timestamp: '2026-10-12 09:00:00.000' })], null),
      )
    const { ctx, emitted, watermarks } = makeCtx({
      watermark: { startDate: '2026-10-01T00:00:00.000Z' },
    })

    const outcome = await runMentionPoll(ctx)

    expect(outcome).toEqual({ complete: true })
    expect(emitted).toHaveLength(2)
    expect(mockedFetchMentionsPage).toHaveBeenNthCalledWith(1, 'test-token', {
      startDate: '2026-10-01T00:00:00.000Z',
      cursor: undefined,
    })
    expect(mockedFetchMentionsPage).toHaveBeenNthCalledWith(2, 'test-token', {
      startDate: '2026-10-01T00:00:00.000Z',
      cursor: 'cursor-2',
    })
    // Intermediate commit resumes via cursor, not startDate — advancing early risks skipping
    // page 2's unseen mentions if Octolens orders newest-first and the run is cut short here.
    expect(watermarks.map(pollFields)).toEqual([
      {
        startDate: '2026-10-01T00:00:00.000Z',
        cursor: 'cursor-2',
        pendingMaxTimestamp: '2026-10-12T08:00:00.000Z',
      },
      { startDate: '2026-10-12T09:00:00.000Z', cursor: null, pendingMaxTimestamp: null },
    ])
  })

  it('does not advance startDate when the run budget runs out mid-pagination, only the cursor', async () => {
    mockedFetchMentionsPage.mockResolvedValue(page([mention()], 'cursor-2'))
    let calls = 0
    const { ctx, watermarks } = makeCtx({
      watermark: { startDate: '2026-10-01T00:00:00.000Z' },
      hasRunBudget: () => calls++ === 0,
    })

    const outcome = await runMentionPoll(ctx)

    expect(outcome).toEqual({ complete: false })
    expect(watermarks.map(pollFields)).toEqual([
      {
        startDate: '2026-10-01T00:00:00.000Z',
        cursor: 'cursor-2',
        pendingMaxTimestamp: '2026-10-12T08:00:00.000Z',
      },
    ])
  })

  it('carries pendingMaxTimestamp across a resumed cycle instead of resetting to startDate', async () => {
    mockedFetchMentionsPage.mockResolvedValueOnce(
      page([mention({ sourceId: 'resume', timestamp: '2026-10-05 00:00:00.000' })], null),
    )
    const { ctx, watermarks } = makeCtx({
      watermark: {
        startDate: '2026-10-01T00:00:00.000Z',
        cursor: 'cursor-2',
        pendingMaxTimestamp: '2026-10-12T08:00:00.000Z',
      },
    })

    await runMentionPoll(ctx)

    // Drains on a page whose own timestamp is older than the earlier run's max — the
    // committed startDate must still be the carried-forward max, not this page's lower value.
    expect(watermarks.map(pollFields)).toEqual([
      {
        startDate: '2026-10-12T08:00:00.000Z',
        cursor: null,
        pendingMaxTimestamp: null,
      },
    ])
  })

  it('resumes from a persisted cursor on the next run without redoing startDate', async () => {
    mockedFetchMentionsPage.mockResolvedValue(page([mention()], null))
    const { ctx } = makeCtx({
      watermark: { startDate: '2026-10-01T00:00:00.000Z', cursor: 'cursor-2' },
    })

    await runMentionPoll(ctx)

    expect(mockedFetchMentionsPage).toHaveBeenCalledWith('test-token', {
      startDate: '2026-10-01T00:00:00.000Z',
      cursor: 'cursor-2',
    })
  })

  it('rejects a credential of the wrong kind', async () => {
    const { ctx } = makeCtx({
      getCredential: async () => ({
        platform: 'github',
        kind: 'github-app',
        data: { appId: 'a', privateKey: 'k' },
      }),
    })

    await expect(runMentionPoll(ctx)).rejects.toThrow(
      'expected octolens-token credential, got github-app',
    )
  })

  it('maps a GitHub-sourced mention to PlatformType.GITHUB end to end', async () => {
    mockedFetchMentionsPage.mockResolvedValue(page([mention()]))
    const { ctx, emitted } = makeCtx()

    await runMentionPoll(ctx)

    const [record] = emitted[0].records as { platform: string }[]
    expect(record.platform).toBe(PlatformType.GITHUB)
  })
})

describe('runMentionPoll first run', () => {
  it('saves its start date before fetching, so a failed first run retries the same window', async () => {
    mockedFetchMentionsPage.mockRejectedValueOnce(new Error('octolens unavailable'))
    const first = makeCtx()

    await expect(runMentionPoll(first.ctx)).rejects.toThrow('octolens unavailable')
    expect(first.watermarks.map(pollFields)).toEqual([INITIAL_CUTOFF])

    vi.setSystemTime(new Date('2026-10-02T00:00:00.000Z'))
    mockedFetchMentionsPage.mockResolvedValue(page([]))
    const retry = makeCtx({ watermark: first.watermarks[0] })

    await runMentionPoll(retry.ctx)
    expect(mockedFetchMentionsPage).toHaveBeenLastCalledWith('test-token', {
      startDate: NOW,
      cursor: undefined,
    })
  })

  it('keeps its first start date when it stops early, so the next run resumes the same window', async () => {
    let calls = 0
    mockedFetchMentionsPage.mockResolvedValue(page([], 'cursor-2'))
    const { ctx, watermarks } = makeCtx({ hasRunBudget: () => calls++ === 0 })

    expect(await runMentionPoll(ctx)).toEqual({ complete: false })
    expect(watermarks[watermarks.length - 1]).toMatchObject({ startDate: NOW, cursor: 'cursor-2' })
  })

  it('starts at now instead of fetching the whole history', async () => {
    mockedFetchMentionsPage.mockResolvedValue(page([]))
    const { ctx, watermarks } = makeCtx()

    expect(await runMentionPoll(ctx)).toEqual({ complete: true })
    expect(mockedFetchMentionsPage).toHaveBeenCalledWith('test-token', {
      startDate: NOW,
      cursor: undefined,
    })
    expect(watermarks[watermarks.length - 1]).toMatchObject({ startDate: NOW, backfill: [] })
  })
})

describe('runMentionPoll multi-keyword mentions', () => {
  it('routes a mention by its first mapped keyword', async () => {
    mockedFetchMentionsPage.mockResolvedValue(
      page([
        mention({
          keywords: [
            { id: 5, keyword: 'unmapped' },
            { id: 7, keyword: 'second' },
            { id: 9, keyword: 'third' },
          ],
        }),
      ]),
    )
    const { ctx, emitted } = makeCtx({
      resolveKeywordTarget: async (keywordId) =>
        keywordId === 5 ? null : target(`segment-${keywordId}`),
    })

    await runMentionPoll(ctx)

    expect(emitted).toHaveLength(1)
    expect(emitted[0].segmentId).toBe('segment-7')
    expect(emitted[0].records[0]).toMatchObject({
      attributes: { keywordId: 7, keyword: 'second' },
    })
  })

  it('never queues a backfill on its own when a keyword is unmapped', async () => {
    mockedFetchMentionsPage.mockResolvedValue(page([mention({ keywordId: 7 })]))
    const { ctx, emitted, watermarks } = makeCtx({ resolveKeywordTarget: async () => null })

    await runMentionPoll(ctx)

    expect(emitted).toEqual([])
    expect(watermarks[watermarks.length - 1]).toEqual(
      expect.not.objectContaining({ unmappedKeywords: expect.anything() }),
    )
    expect(watermarks[watermarks.length - 1]).toMatchObject({ backfill: [] })
  })
})

describe('runMentionPoll keyword backfill (queued manually)', () => {
  const MAPPED = async (keywordId: number) => (keywordId === 7 ? target('segment-7') : null)

  it("fetches a queued keyword's whole history, filtered to that keyword", async () => {
    mockedFetchMentionsPage
      .mockResolvedValueOnce(page([]))
      .mockResolvedValueOnce(page([mention({ keywordId: 7, sourceId: 'old-1' })]))
    const { ctx, emitted, watermarks } = makeCtx({
      resolveKeywordTarget: MAPPED,
      watermark: {
        startDate: '2026-10-12T08:00:00.000Z',
        backfill: [{ keywordId: 7, cursor: null }],
      },
    })

    expect(await runMentionPoll(ctx)).toEqual({ complete: true })
    expect(mockedFetchMentionsPage).toHaveBeenLastCalledWith('test-token', {
      keywordIds: [7],
      cursor: undefined,
    })
    expect(emitted).toEqual([
      expect.objectContaining({ segmentId: 'segment-7', records: [expect.anything()] }),
    ])
    expect(watermarks[watermarks.length - 1]).toMatchObject({
      startDate: '2026-10-12T08:00:00.000Z',
      backfill: [],
    })
  })

  it('resumes a long keyword backfill from its cursor on the next run', async () => {
    let calls = 0
    mockedFetchMentionsPage
      .mockResolvedValueOnce(page([]))
      .mockResolvedValueOnce(page([mention({ keywordId: 7 })], 'backfill-cursor'))
    const { ctx, watermarks } = makeCtx({
      resolveKeywordTarget: MAPPED,
      watermark: {
        startDate: '2026-10-12T08:00:00.000Z',
        backfill: [{ keywordId: 7, cursor: null }],
      },
      hasRunBudget: () => calls++ < 2,
    })

    const outcome = await runMentionPoll(ctx)

    expect(outcome).toEqual({ complete: true })
    expect(watermarks[watermarks.length - 1]).toMatchObject({
      backfill: [{ keywordId: 7, cursor: 'backfill-cursor' }],
    })

    mockedFetchMentionsPage.mockReset()
    mockedFetchMentionsPage.mockResolvedValueOnce(page([])).mockResolvedValueOnce(page([]))
    const next = makeCtx({
      resolveKeywordTarget: MAPPED,
      watermark: watermarks[watermarks.length - 1] ?? null,
    })

    expect(await runMentionPoll(next.ctx)).toEqual({ complete: true })
    expect(mockedFetchMentionsPage).toHaveBeenLastCalledWith('test-token', {
      keywordIds: [7],
      cursor: 'backfill-cursor',
    })
    expect(next.watermarks[next.watermarks.length - 1]).toMatchObject({ backfill: [] })
  })

  it('does not start a backfill until the regular poll has drained', async () => {
    let calls = 0
    mockedFetchMentionsPage.mockResolvedValue(page([], 'poll-cursor'))
    const { ctx, watermarks } = makeCtx({
      resolveKeywordTarget: MAPPED,
      watermark: {
        startDate: '2026-10-12T08:00:00.000Z',
        backfill: [{ keywordId: 7, cursor: null }],
      },
      hasRunBudget: () => calls++ === 0,
    })

    expect(await runMentionPoll(ctx)).toEqual({ complete: false })
    expect(mockedFetchMentionsPage).toHaveBeenCalledTimes(1)
    expect(watermarks[watermarks.length - 1]).toMatchObject({
      cursor: 'poll-cursor',
      backfill: [{ keywordId: 7, cursor: null }],
    })
  })

  it('routes a backfilled mention like a polled one, by its first mapped keyword', async () => {
    mockedFetchMentionsPage.mockResolvedValueOnce(page([])).mockResolvedValueOnce(
      page([
        mention({
          keywords: [
            { id: 3, keyword: 'other' },
            { id: 7, keyword: 'backfilled' },
          ],
        }),
      ]),
    )
    const { ctx, emitted } = makeCtx({
      resolveKeywordTarget: async (keywordId) => target(`segment-${keywordId}`),
      watermark: {
        startDate: '2026-10-12T08:00:00.000Z',
        backfill: [{ keywordId: 7, cursor: null }],
      },
    })

    await runMentionPoll(ctx)

    expect(emitted).toEqual([expect.objectContaining({ segmentId: 'segment-3' })])
  })

  it('skips a null entry on a backfill page instead of blocking the backfill', async () => {
    mockedFetchMentionsPage
      .mockResolvedValueOnce(page([]))
      .mockResolvedValueOnce(
        page([null as unknown as OctolensMention, mention({ keywordId: 7, sourceId: 'ok' })]),
      )
    const { ctx, emitted, watermarks } = makeCtx({
      resolveKeywordTarget: MAPPED,
      watermark: {
        startDate: '2026-10-12T08:00:00.000Z',
        backfill: [{ keywordId: 7, cursor: null }],
      },
    })

    expect(await runMentionPoll(ctx)).toEqual({ complete: true })
    expect(emitted).toHaveLength(1)
    expect(watermarks[watermarks.length - 1]).toMatchObject({ backfill: [] })
  })

  it('drops a queued backfill whose keyword has no mapping', async () => {
    mockedFetchMentionsPage.mockResolvedValue(page([]))
    const { ctx, watermarks } = makeCtx({
      resolveKeywordTarget: async () => null,
      watermark: {
        startDate: '2026-10-12T08:00:00.000Z',
        backfill: [{ keywordId: 7, cursor: 'half-way' }],
      },
    })

    expect(await runMentionPoll(ctx)).toEqual({ complete: true })
    expect(mockedFetchMentionsPage).toHaveBeenCalledTimes(1)
    expect(watermarks[watermarks.length - 1]).toMatchObject({ backfill: [] })
  })

  it('stops a backfill after a page cap so it stays within the shared request budget', async () => {
    mockedFetchMentionsPage.mockResolvedValueOnce(page([]))
    for (let i = 1; i <= 25; i++) {
      mockedFetchMentionsPage.mockResolvedValueOnce(page([], `backfill-${i}`))
    }
    const { ctx, watermarks } = makeCtx({
      resolveKeywordTarget: async () => target('segment-7'),
      watermark: {
        startDate: '2026-10-12T08:00:00.000Z',
        backfill: [{ keywordId: 7, cursor: null }],
      },
    })

    expect(await runMentionPoll(ctx)).toEqual({ complete: true })
    expect(mockedFetchMentionsPage).toHaveBeenCalledTimes(9)
    expect(watermarks[watermarks.length - 1]).toMatchObject({
      backfill: [{ keywordId: 7, cursor: 'backfill-8' }],
    })
  })

  it('ends a backfill whose cursor stops advancing instead of looping forever', async () => {
    mockedFetchMentionsPage.mockResolvedValueOnce(page([])).mockResolvedValueOnce(page([], 'stuck'))
    const { ctx, watermarks } = makeCtx({
      resolveKeywordTarget: async () => target('segment-7'),
      watermark: {
        startDate: '2026-10-12T08:00:00.000Z',
        backfill: [{ keywordId: 7, cursor: 'stuck' }],
      },
    })

    expect(await runMentionPoll(ctx)).toEqual({ complete: true })
    expect(mockedFetchMentionsPage).toHaveBeenCalledTimes(2)
    expect(watermarks[watermarks.length - 1]).toMatchObject({ backfill: [] })
  })

  it('commits a snapshot of the backfill queue, not a reference later pages change', async () => {
    mockedFetchMentionsPage
      .mockResolvedValueOnce(page([]))
      .mockResolvedValueOnce(page([], 'next'))
      .mockResolvedValueOnce(page([]))
    const { ctx, watermarks } = makeCtx({
      resolveKeywordTarget: async () => target('segment-7'),
      watermark: {
        startDate: '2026-10-12T08:00:00.000Z',
        backfill: [{ keywordId: 7, cursor: null }],
      },
    })

    await runMentionPoll(ctx)

    expect(watermarks.map((w) => w.backfill)).toEqual([
      [{ keywordId: 7, cursor: null }],
      [{ keywordId: 7, cursor: 'next' }],
      [],
    ])
  })

  it('ignores malformed backfill entries in a stored watermark', async () => {
    mockedFetchMentionsPage.mockResolvedValue(page([]))
    const { ctx, watermarks } = makeCtx({
      resolveKeywordTarget: async () => null,
      watermark: {
        startDate: '2026-10-12T08:00:00.000Z',
        backfill: [
          { keywordId: 'x', cursor: null },
          { keywordId: 9, cursor: 5 },
        ],
      },
    })

    expect(await runMentionPoll(ctx)).toEqual({ complete: true })
    expect(mockedFetchMentionsPage).toHaveBeenCalledTimes(1)
    expect(watermarks[watermarks.length - 1]).toMatchObject({ backfill: [] })
  })
})

describe('runMentionPoll page cap', () => {
  const START = '2026-10-01T00:00:00.000Z'
  const QUEUED = [{ keywordId: 7, cursor: null }]

  it('stops the regular poll at 20 pages and keeps the cycle, without starting the backfill', async () => {
    for (let i = 1; i <= 25; i++) {
      mockedFetchMentionsPage.mockResolvedValueOnce(page([mention()], `cursor-${i}`))
    }
    const { ctx, watermarks } = makeCtx({ watermark: { startDate: START, backfill: QUEUED } })

    expect(await runMentionPoll(ctx)).toEqual({ complete: false, resumeAtCadence: true })
    expect(mockedFetchMentionsPage).toHaveBeenCalledTimes(20)
    expect(mockedFetchMentionsPage).not.toHaveBeenCalledWith('test-token', {
      keywordIds: [7],
      cursor: undefined,
    })
    expect(watermarks[watermarks.length - 1]).toEqual({
      startDate: START,
      cursor: 'cursor-20',
      pendingMaxTimestamp: '2026-10-12T08:00:00.000Z',
      backfill: QUEUED,
    })
  })

  it('resumes the capped cycle from its cursor on the next run, then runs the backfill', async () => {
    mockedFetchMentionsPage
      .mockResolvedValueOnce(page([mention()], null))
      .mockResolvedValueOnce(page([]))
    const { ctx, watermarks } = makeCtx({
      watermark: {
        startDate: START,
        cursor: 'cursor-20',
        pendingMaxTimestamp: '2026-10-12T08:00:00.000Z',
        backfill: QUEUED,
      },
    })

    expect(await runMentionPoll(ctx)).toEqual({ complete: true })
    expect(mockedFetchMentionsPage.mock.calls.map(([, params]) => params)).toEqual([
      { startDate: START, cursor: 'cursor-20' },
      { keywordIds: [7], cursor: undefined },
    ])
    expect(watermarks[watermarks.length - 1]).toEqual({
      startDate: '2026-10-12T08:00:00.000Z',
      cursor: null,
      pendingMaxTimestamp: null,
      backfill: [],
    })
  })

  it('finishes a cycle that drains exactly on the 20th page', async () => {
    for (let i = 1; i < 20; i++) {
      mockedFetchMentionsPage.mockResolvedValueOnce(page([], `cursor-${i}`))
    }
    mockedFetchMentionsPage.mockResolvedValueOnce(page([mention()], null))
    const { ctx, watermarks } = makeCtx({ watermark: { startDate: START } })

    expect(await runMentionPoll(ctx)).toEqual({ complete: true })
    expect(mockedFetchMentionsPage).toHaveBeenCalledTimes(20)
    expect(watermarks[watermarks.length - 1]).toMatchObject({
      startDate: '2026-10-12T08:00:00.000Z',
      cursor: null,
    })
  })
})
