import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { CredlyActivityType } from '@crowd/integrations/src/integrations/credly/types'
import type { Logger } from '@crowd/logging'

import { ProviderContractError, ProviderUnavailableError } from '../../../http/errors'
import type { Credential, SyncContext } from '../../../types'
import { fetchBadgesPage } from '../apiClient'
import type { BadgesPageParams, CredlyPage } from '../apiClient'
import type { CredlyBadge } from '../badge'
import { MAX_PAGES_PER_RUN, MAX_WINDOW_MS, WINDOW_LAG_MS, runBadgePoll } from './badgePoll'

vi.mock('../apiClient', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../apiClient')>()
  return { ...actual, fetchBadgesPage: vi.fn() }
})

const mockedFetchBadgesPage = vi.mocked(fetchBadgesPage)

const NOW = new Date('2026-10-23T12:00:00.000Z')
const HORIZON = new Date(NOW.getTime() - WINDOW_LAG_MS).toISOString()
const DAY_MS = 24 * 60 * 60 * 1000
const ORGANIZATION_ID = 'f4b8d042-0072-4a1a-8d00-260b513026e8'

const CREDENTIAL: Credential = {
  platform: 'credly',
  kind: 'credly-oauth-client',
  data: { clientId: 'id', clientSecret: 'secret', organizationId: ORGANIZATION_ID },
}

const LOG = { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() } as unknown as Logger

function badge(overrides: Partial<CredlyBadge> = {}): CredlyBadge {
  return {
    id: 'badge-1',
    state: 'accepted',
    issued_at: '2026-10-22T00:00:00.000Z',
    expires_at: null,
    state_updated_at: '2026-10-22T10:00:00.000Z',
    recipient_email: 'maria.rossi@example.org',
    issued_to: 'Maria Rossi',
    badge_url: 'https://www.credly.com/badges/badge-1',
    user: { id: 'user-1', url: 'https://www.credly.com/users/maria-rossi' },
    badge_template: { id: 'template-1', name: 'CKA: Certified Kubernetes Administrator' },
    ...overrides,
  }
}

function page(data: CredlyBadge[], current = 1, totalPages = 1): CredlyPage<CredlyBadge> {
  return {
    data,
    metadata: {
      count: data.length,
      current_page: current,
      total_count: data.length,
      total_pages: totalPages,
    },
  }
}

function makeCtx(watermark: Record<string, unknown> | null = null, hasRunBudget = () => true) {
  const emitted: unknown[][] = []
  const watermarks: Record<string, unknown>[] = []
  const ctx: SyncContext = {
    channel: { channelId: ORGANIZATION_ID, channelName: ORGANIZATION_ID },
    integrationId: 'integration-1',
    watermark,
    emit: async (records) => {
      emitted.push(records)
    },
    commitWatermark: async (next) => {
      watermarks.push(next)
    },
    hasRunBudget,
    http: {} as SyncContext['http'],
    log: LOG,
    getCredential: async () => CREDENTIAL,
    deps: undefined,
  }
  return { ctx, emitted, watermarks, lastWatermark: () => watermarks[watermarks.length - 1] }
}

function requestedParams(): BadgesPageParams[] {
  return mockedFetchBadgesPage.mock.calls.map(([, params]) => params)
}

function types(records: unknown[]): string[] {
  return records.map((record) => (record as { type: string }).type)
}

describe('runBadgePoll', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(NOW)
    mockedFetchBadgesPage.mockReset()
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.clearAllMocks()
  })

  it('polls from one day back on the first run and emits issued plus state activities', async () => {
    mockedFetchBadgesPage.mockResolvedValueOnce(page([badge()]))
    const { ctx, emitted, lastWatermark } = makeCtx()

    const outcome = await runBadgePoll(ctx)

    expect(outcome).toEqual({ complete: true })
    expect(requestedParams()).toEqual([
      {
        organizationId: ORGANIZATION_ID,
        window: { min: new Date(NOW.getTime() - DAY_MS).toISOString(), max: HORIZON },
        page: 1,
      },
    ])
    expect(emitted).toHaveLength(1)
    expect(types(emitted[0])).toEqual([
      CredlyActivityType.ISSUED_BADGE,
      CredlyActivityType.ACCEPTED_BADGE,
    ])
    expect(lastWatermark()).toEqual({ since: HORIZON, until: null, page: 1 })
  })

  it('resumes a cut window from the saved page and advances since when it drains', async () => {
    const since = '2026-10-20T00:00:00.000Z'
    mockedFetchBadgesPage.mockResolvedValueOnce(page([badge()], 3, 3))
    const { ctx, lastWatermark } = makeCtx({ since, until: HORIZON, page: 3 })

    const outcome = await runBadgePoll(ctx)

    expect(outcome).toEqual({ complete: true })
    expect(requestedParams()).toEqual([
      { organizationId: ORGANIZATION_ID, window: { min: since, max: HORIZON }, page: 3 },
    ])
    expect(lastWatermark()).toEqual({ since: HORIZON, until: null, page: 1 })
  })

  it('walks a long gap in seven-day windows and stops when it reaches the horizon', async () => {
    const since = new Date(Date.parse(HORIZON) - MAX_WINDOW_MS - 2 * DAY_MS).toISOString()
    const firstUntil = new Date(Date.parse(since) + MAX_WINDOW_MS).toISOString()
    const lastSeen = new Date(Date.parse(since) + DAY_MS).toISOString()
    mockedFetchBadgesPage
      .mockResolvedValueOnce(page([badge({ id: 'badge-1', state_updated_at: lastSeen })], 1, 2))
      .mockResolvedValueOnce(page([badge({ id: 'badge-2', state_updated_at: lastSeen })], 1, 1))
      .mockResolvedValueOnce(page([], 1, 0))
    const { ctx, emitted, lastWatermark } = makeCtx({ since })

    const outcome = await runBadgePoll(ctx)

    expect(outcome).toEqual({ complete: true })
    expect(requestedParams().map(({ window, page: p }) => ({ ...window, page: p }))).toEqual([
      { min: since, max: firstUntil, page: 1 },
      { min: lastSeen, max: firstUntil, page: 1 },
      { min: firstUntil, max: HORIZON, page: 1 },
    ])
    expect(emitted).toHaveLength(2)
    expect(lastWatermark()).toEqual({ since: HORIZON, until: null, page: 1 })
  })

  it('follows the last badge seen, so a badge leaving the window mid-walk skips nothing', async () => {
    const since = '2026-10-22T00:00:00.000Z'
    const at = (hour: number) => `2026-10-22T${String(hour).padStart(2, '0')}:00:00.500Z`
    const live = ['a', 'b', 'c', 'd'].map((id, index) =>
      badge({ id, state_updated_at: at(index + 1) }),
    )
    const perPage = 2
    mockedFetchBadgesPage.mockImplementation(async (_http, params) => {
      const inWindow = live
        .filter((b) => Date.parse(b.state_updated_at) >= Date.parse(params.window.min))
        .filter((b) => Date.parse(b.state_updated_at) <= Date.parse(params.window.max))
      const data = inWindow.slice((params.page - 1) * perPage, params.page * perPage)
      if (mockedFetchBadgesPage.mock.calls.length === 1) {
        live[0].state_updated_at = '2026-10-23T11:58:00.000Z'
      }
      return page(data, params.page, Math.ceil(inWindow.length / perPage))
    })
    const { ctx, emitted } = makeCtx({ since })

    await runBadgePoll(ctx)

    expect(requestedParams().map(({ window, page: p }) => ({ min: window.min, page: p }))).toEqual([
      { min: since, page: 1 },
      { min: '2026-10-22T02:00:00.000Z', page: 1 },
      { min: '2026-10-22T03:00:00.000Z', page: 1 },
    ])
    const issued = emitted
      .flat()
      .filter((record) => (record as { type: string }).type === CredlyActivityType.ISSUED_BADGE)
      .map((record) => (record as { sourceId: string }).sourceId)
    expect(issued).toEqual(['a', 'b', 'b', 'c', 'c', 'd'])
  })

  it('falls back to page numbers only for badges sharing the same second', async () => {
    const since = '2026-10-22T00:00:00.000Z'
    const tie = '2026-10-22T10:00:00.250Z'
    mockedFetchBadgesPage
      .mockResolvedValueOnce(page([badge({ id: 'a', state_updated_at: tie })], 1, 2))
      .mockResolvedValueOnce(page([badge({ id: 'a', state_updated_at: tie })], 1, 2))
      .mockResolvedValueOnce(page([badge({ id: 'b', state_updated_at: tie })], 2, 2))
    const { ctx, lastWatermark } = makeCtx({ since })

    const outcome = await runBadgePoll(ctx)

    expect(outcome).toEqual({ complete: true })
    expect(requestedParams().map(({ window, page: p }) => ({ min: window.min, page: p }))).toEqual([
      { min: since, page: 1 },
      { min: '2026-10-22T10:00:00.000Z', page: 1 },
      { min: '2026-10-22T10:00:00.000Z', page: 2 },
    ])
    expect(lastWatermark()).toEqual({ since: HORIZON, until: null, page: 1 })
  })

  it('halves a window that Credly keeps failing and retries its first half', async () => {
    const since = new Date(Date.parse(HORIZON) - MAX_WINDOW_MS).toISOString()
    const half = new Date(Date.parse(since) + MAX_WINDOW_MS / 2).toISOString()
    mockedFetchBadgesPage
      .mockRejectedValueOnce(new ProviderUnavailableError('credly answered 500', { status: 500 }))
      .mockResolvedValueOnce(page([badge()]))
      .mockResolvedValueOnce(page([], 1, 0))
    const { ctx, watermarks, lastWatermark } = makeCtx({ since })

    const outcome = await runBadgePoll(ctx)

    expect(outcome).toEqual({ complete: true })
    expect(requestedParams().map(({ window }) => window)).toEqual([
      { min: since, max: HORIZON },
      { min: since, max: half },
      { min: half, max: HORIZON },
    ])
    expect(watermarks).toContainEqual({ since, until: half, page: 1 })
    expect(lastWatermark()).toEqual({ since: HORIZON, until: null, page: 1 })
    expect(LOG.warn).toHaveBeenCalledWith(
      expect.objectContaining({ since, until: HORIZON, halvedUntil: half }),
      'credly badge window failed, retrying its first half',
    )
  })

  it('gives up on a window that cannot be halved further, keeping the last watermark', async () => {
    const since = new Date(Date.parse(HORIZON) - 90 * 60 * 1000).toISOString()
    mockedFetchBadgesPage.mockRejectedValue(
      new ProviderUnavailableError('credly answered 500', { status: 500 }),
    )
    const { ctx, lastWatermark } = makeCtx({ since })

    await expect(runBadgePoll(ctx)).rejects.toBeInstanceOf(ProviderUnavailableError)

    expect(mockedFetchBadgesPage).toHaveBeenCalledTimes(1)
    expect(lastWatermark()).toEqual({ since, until: HORIZON, page: 1 })
  })

  it.each([
    ['a contract error', new ProviderContractError('unexpected shape')],
    ['an unreachable provider', new ProviderUnavailableError('no response from provider')],
  ])('does not split a window on %s', async (_label, err) => {
    mockedFetchBadgesPage.mockRejectedValueOnce(err)
    const { ctx } = makeCtx()

    await expect(runBadgePoll(ctx)).rejects.toBe(err)

    expect(mockedFetchBadgesPage).toHaveBeenCalledTimes(1)
  })

  it('pauses at the page cap with the last badge seen saved, so progress persists in steps', async () => {
    const base = Date.parse('2026-10-20T00:00:00.000Z')
    mockedFetchBadgesPage.mockImplementation(async () => {
      const call = mockedFetchBadgesPage.mock.calls.length
      const stateUpdatedAt = new Date(base + call * 60_000).toISOString()
      return page([badge({ id: `badge-${call}`, state_updated_at: stateUpdatedAt })], 1, 1000)
    })
    const { ctx, emitted, lastWatermark } = makeCtx({ since: new Date(base).toISOString() })

    const outcome = await runBadgePoll(ctx)

    expect(outcome).toEqual({ complete: false })
    expect(mockedFetchBadgesPage).toHaveBeenCalledTimes(MAX_PAGES_PER_RUN)
    expect(emitted).toHaveLength(MAX_PAGES_PER_RUN)
    expect(lastWatermark()).toEqual({
      since: new Date(base + MAX_PAGES_PER_RUN * 60_000).toISOString(),
      until: HORIZON,
      page: 1,
    })
  })

  it('defers when the run budget is exhausted mid-window', async () => {
    mockedFetchBadgesPage.mockResolvedValue(page([badge()], 1, 5))
    let budget = 1
    const { ctx, lastWatermark } = makeCtx(
      { since: '2026-10-22T00:00:00.000Z' },
      () => budget-- > 0,
    )

    const outcome = await runBadgePoll(ctx)

    expect(outcome).toEqual({ complete: false })
    expect(mockedFetchBadgesPage).toHaveBeenCalledTimes(1)
    expect(lastWatermark()).toEqual({ since: '2026-10-22T10:00:00.000Z', until: HORIZON, page: 1 })
  })

  it('leaves the saved watermark untouched when already caught up', async () => {
    const { ctx, watermarks } = makeCtx({ since: HORIZON })

    const outcome = await runBadgePoll(ctx)

    expect(outcome).toEqual({ complete: true })
    expect(mockedFetchBadgesPage).not.toHaveBeenCalled()
    expect(watermarks).toEqual([])
  })

  it('commits the first-run watermark before fetching, so a failed first run retries the same window', async () => {
    mockedFetchBadgesPage.mockRejectedValueOnce(new ProviderContractError('unexpected shape'))
    const { ctx, watermarks } = makeCtx()

    await expect(runBadgePoll(ctx)).rejects.toBeInstanceOf(ProviderContractError)

    expect(watermarks[0]).toEqual({
      since: new Date(NOW.getTime() - DAY_MS).toISOString(),
      until: null,
      page: 1,
    })
  })

  it('emits expired-badge only for accepted badges whose expiry has already passed', async () => {
    mockedFetchBadgesPage.mockResolvedValueOnce(
      page([
        badge({ id: 'expired', expires_at: '2026-10-01T00:00:00.000Z' }),
        badge({ id: 'live', expires_at: '2028-10-01T00:00:00.000Z' }),
        badge({ id: 'pending', state: 'pending', state_updated_at: null }),
        badge({ id: 'revoked', state: 'revoked', expires_at: '2026-10-01T00:00:00.000Z' }),
      ]),
    )
    const { ctx, emitted } = makeCtx()

    await runBadgePoll(ctx)

    expect(types(emitted[0])).toEqual([
      CredlyActivityType.ISSUED_BADGE,
      CredlyActivityType.ACCEPTED_BADGE,
      CredlyActivityType.EXPIRED_BADGE,
      CredlyActivityType.ISSUED_BADGE,
      CredlyActivityType.ACCEPTED_BADGE,
      CredlyActivityType.ISSUED_BADGE,
      CredlyActivityType.ISSUED_BADGE,
      CredlyActivityType.REVOKED_BADGE,
    ])
  })

  it('skips a badge the mapper cannot process and keeps the rest of the page', async () => {
    mockedFetchBadgesPage.mockResolvedValueOnce(
      page([badge({ id: 'broken', issued_at: 'not a date' }), badge({ id: 'fine' })]),
    )
    const { ctx, emitted } = makeCtx()

    const outcome = await runBadgePoll(ctx)

    expect(outcome).toEqual({ complete: true })
    expect(emitted[0].map((record) => (record as { sourceId: string }).sourceId)).toEqual([
      'fine',
      'fine',
    ])
    expect(LOG.warn).toHaveBeenCalledWith(
      expect.objectContaining({ badgeId: 'broken' }),
      'skipping credly badge: failed to map (malformed provider data)',
    )
  })

  it('keeps paginating instead of moving since into the second of until', async () => {
    const since = '2026-10-22T00:00:00.000Z'
    const lastSecond = HORIZON.slice(0, 19) + '.900Z'
    mockedFetchBadgesPage
      .mockResolvedValueOnce(page([badge({ id: 'a', state_updated_at: lastSecond })], 1, 2))
      .mockResolvedValueOnce(page([badge({ id: 'b', state_updated_at: lastSecond })], 2, 2))
    const { ctx, lastWatermark } = makeCtx({ since })

    const outcome = await runBadgePoll(ctx)

    expect(outcome).toEqual({ complete: true })
    expect(requestedParams().map(({ window, page: p }) => ({ min: window.min, page: p }))).toEqual([
      { min: since, page: 1 },
      { min: since, page: 2 },
    ])
    expect(lastWatermark()).toEqual({ since: HORIZON, until: null, page: 1 })
  })

  it('ignores a saved window whose bounds fall in the same second', async () => {
    mockedFetchBadgesPage.mockResolvedValueOnce(page([], 1, 0))
    const since = '2026-10-23T11:00:00.000Z'
    const { ctx } = makeCtx({ since, until: '2026-10-23T11:00:00.900Z', page: 2 })

    await runBadgePoll(ctx)

    expect(requestedParams()).toEqual([
      { organizationId: ORGANIZATION_ID, window: { min: since, max: HORIZON }, page: 1 },
    ])
  })

  it('ignores a saved window that does not sit after since', async () => {
    mockedFetchBadgesPage.mockResolvedValueOnce(page([], 1, 0))
    const { ctx } = makeCtx({
      since: '2026-10-22T00:00:00.000Z',
      until: '2026-10-21T00:00:00.000Z',
      page: 7,
    })

    await runBadgePoll(ctx)

    expect(requestedParams()).toEqual([
      {
        organizationId: ORGANIZATION_ID,
        window: { min: '2026-10-22T00:00:00.000Z', max: HORIZON },
        page: 1,
      },
    ])
  })
})
