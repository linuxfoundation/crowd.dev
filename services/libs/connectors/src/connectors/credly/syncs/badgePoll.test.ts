import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type { ICredlyBadgeInput, ICredlyBadgeRow } from '@crowd/data-access-layer/src/credly/badges'
import { CredlyActivityType } from '@crowd/integrations/src/integrations/credly/types'
import type { Logger } from '@crowd/logging'

import { ProviderContractError, ProviderUnavailableError } from '../../../http/errors'
import type { Credential, SyncContext } from '../../../types'
import { fetchBadge, fetchBadgesPage } from '../apiClient'
import type { BadgesPageParams, CredlyPage } from '../apiClient'
import type { CredlyBadge } from '../badge'
import type { CredlyDeps, CredlyTemplateRouting } from '../deps'
import {
  MAX_PAGES_PER_RUN,
  MAX_REPLACEMENT_HOPS,
  MAX_WINDOW_MS,
  WINDOW_LAG_MS,
  runBadgePoll,
} from './badgePoll'

vi.mock('../apiClient', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../apiClient')>()
  return { ...actual, fetchBadgesPage: vi.fn(), fetchBadge: vi.fn() }
})

const mockedFetchBadgesPage = vi.mocked(fetchBadgesPage)
const mockedFetchBadge = vi.mocked(fetchBadge)

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

const DEFAULT_TARGET = { segmentId: 'seg-default', integrationId: 'int-default', locked: false }
const DEFAULT_ROUTING: CredlyTemplateRouting = {
  targets: new Map([['template-1', DEFAULT_TARGET]]),
  staleTemplateIds: [],
}

function storedBadge(overrides: Partial<ICredlyBadgeRow> = {}): ICredlyBadgeRow {
  return {
    badgeId: 'badge-1',
    badgeTemplateId: 'template-1',
    badgeTemplateName: 'CKA: Certified Kubernetes Administrator',
    badgeTemplateUrl: null,
    state: 'accepted',
    stateUpdatedAt: '2026-10-22T10:00:00.000Z',
    issuedAt: '2026-10-22T00:00:00.000Z',
    expiresAt: null,
    replacementBadgeId: null,
    replacesBadgeId: null,
    segmentId: 'seg-stored',
    integrationId: 'int-stored',
    recipientEmail: 'maria.rossi@example.org',
    issuedTo: 'Maria Rossi',
    expiredEmittedAt: null,
    firstSeenAt: '2026-10-22T12:00:00.000Z',
    lastSeenAt: '2026-10-22T12:00:00.000Z',
    ...overrides,
  }
}

function makeCtx(
  watermark: Record<string, unknown> | null = null,
  hasRunBudget = () => true,
  routing: CredlyTemplateRouting = DEFAULT_ROUTING,
  emitEnabled = true,
  stored: ICredlyBadgeRow[] = [],
) {
  const emitted: unknown[][] = []
  const emits: { records: unknown[]; segmentId?: string; integrationId?: string }[] = []
  const watermarks: Record<string, unknown>[] = []
  const saved: ICredlyBadgeInput[] = []
  const deps: CredlyDeps = {
    loadTemplateRouting: vi.fn(async () => routing),
    lockTemplate: vi.fn(async () => true),
    findBadges: vi.fn(async (ids: string[]) => stored.filter((row) => ids.includes(row.badgeId))),
    saveBadges: vi.fn(async (rows: ICredlyBadgeInput[]) => {
      saved.push(...rows)
    }),
    listDueExpiries: vi.fn(async () => []),
    markExpiriesEmitted: vi.fn(async () => 0),
  }
  const ctx: SyncContext<CredlyDeps> = {
    channel: { channelId: ORGANIZATION_ID, channelName: ORGANIZATION_ID },
    integrationId: 'integration-1',
    watermark,
    emit: async (records, overrides) => {
      emitted.push(records)
      emits.push({ records, ...overrides })
    },
    commitWatermark: async (next) => {
      watermarks.push(next)
    },
    emitEnabled,
    hasRunBudget,
    http: {} as SyncContext['http'],
    log: LOG,
    getCredential: async () => CREDENTIAL,
    deps,
  }
  return {
    ctx,
    emitted,
    emits,
    deps,
    saved,
    watermarks,
    lastWatermark: () => watermarks[watermarks.length - 1],
  }
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
    mockedFetchBadge.mockReset()
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

  it('stores a new badge with the routing target and leaves expiries to the expiry sync', async () => {
    mockedFetchBadgesPage.mockResolvedValueOnce(
      page([
        badge({ id: 'expired', expires_at: '2026-10-01T00:00:00.000Z' }),
        badge({ id: 'pending', state: 'pending', state_updated_at: null }),
        badge({ id: 'revoked', state: 'revoked', expires_at: '2026-10-01T00:00:00.000Z' }),
      ]),
    )
    const { ctx, emitted, saved, deps } = makeCtx()

    await runBadgePoll(ctx)

    expect(types(emitted[0])).toEqual([
      CredlyActivityType.ISSUED_BADGE,
      CredlyActivityType.ACCEPTED_BADGE,
      CredlyActivityType.ISSUED_BADGE,
      CredlyActivityType.REVOKED_BADGE,
    ])
    expect(deps.findBadges).toHaveBeenCalledWith(['expired', 'pending', 'revoked'])
    expect(saved.map((row) => [row.badgeId, row.state, row.segmentId, row.expiresAt])).toEqual([
      ['expired', 'accepted', 'seg-default', '2026-10-01T00:00:00.000Z'],
      ['pending', 'pending', 'seg-default', null],
      ['revoked', 'revoked', 'seg-default', '2026-10-01T00:00:00.000Z'],
    ])
    expect(saved[0]).toMatchObject({
      badgeTemplateName: 'CKA: Certified Kubernetes Administrator',
      recipientEmail: 'maria.rossi@example.org',
      issuedTo: 'Maria Rossi',
    })
    expect(LOG.info).toHaveBeenCalledWith(
      expect.objectContaining({ routed: 2, pendingStored: 1, stateChanges: 0, unchanged: 0 }),
      expect.any(String),
    )
  })

  it('emits nothing for a known badge whose state did not change, but marks it seen', async () => {
    mockedFetchBadgesPage.mockResolvedValueOnce(
      page([badge({ state_updated_at: '2026-10-27T10:00:00.000Z' })]),
    )
    const { ctx, emits, saved, deps } = makeCtx(null, () => true, DEFAULT_ROUTING, true, [
      storedBadge(),
    ])

    await runBadgePoll(ctx)

    expect(emits).toEqual([])
    expect(deps.lockTemplate).toHaveBeenCalledWith('template-1', {
      segmentId: 'seg-stored',
      integrationId: 'int-stored',
      locked: true,
    })
    expect(saved.map((row) => [row.badgeId, row.segmentId, row.stateUpdatedAt])).toEqual([
      ['badge-1', 'seg-stored', '2026-10-27T10:00:00.000Z'],
    ])
    expect(LOG.info).toHaveBeenCalledWith(
      expect.objectContaining({ unchanged: 1, routed: 0 }),
      expect.any(String),
    )
  })

  it('emits only the state activity on the stored segment when a known badge changes state', async () => {
    mockedFetchBadgesPage.mockResolvedValueOnce(
      page([
        badge({
          state: 'replaced',
          state_updated_at: '2026-10-27T10:00:00.000Z',
          replacement_badge_id: 'badge-2',
        }),
        badge({ id: 'badge-2', replaces_badge_id: 'badge-1' }),
      ]),
    )
    const { ctx, emits, saved, deps } = makeCtx(null, () => true, DEFAULT_ROUTING, true, [
      storedBadge(),
    ])

    await runBadgePoll(ctx)

    expect(
      emits.map(({ records, segmentId, integrationId }) => ({
        types: types(records),
        segmentId,
        integrationId,
      })),
    ).toEqual([
      {
        types: [CredlyActivityType.REPLACED_BADGE],
        segmentId: 'seg-stored',
        integrationId: 'int-stored',
      },
      {
        types: [CredlyActivityType.ISSUED_BADGE, CredlyActivityType.ACCEPTED_BADGE],
        segmentId: 'seg-default',
        integrationId: 'int-default',
      },
    ])
    expect(deps.lockTemplate).toHaveBeenCalledTimes(1)
    expect(deps.lockTemplate).toHaveBeenCalledWith('template-1', {
      segmentId: 'seg-stored',
      integrationId: 'int-stored',
      locked: true,
    })
    expect(
      saved.map((row) => [row.badgeId, row.state, row.segmentId, row.replacementBadgeId]),
    ).toEqual([
      ['badge-1', 'replaced', 'seg-stored', 'badge-2'],
      ['badge-2', 'accepted', 'seg-default', null],
    ])
    expect(LOG.info).toHaveBeenCalledWith(
      expect.objectContaining({ stateChanges: 1, routed: 1 }),
      expect.any(String),
    )
  })

  it('emits issued and accepted for a stored pending badge once it is accepted', async () => {
    mockedFetchBadgesPage.mockResolvedValueOnce(page([badge()]))
    const { ctx, emitted, emits } = makeCtx(null, () => true, DEFAULT_ROUTING, true, [
      storedBadge({ state: 'pending', stateUpdatedAt: null }),
    ])

    await runBadgePoll(ctx)

    expect(emitted).toHaveLength(1)
    expect(types(emitted[0])).toEqual([
      CredlyActivityType.ISSUED_BADGE,
      CredlyActivityType.ACCEPTED_BADGE,
    ])
    expect(emits[0]).toMatchObject({ segmentId: 'seg-stored', integrationId: 'int-stored' })
  })

  it('locks the template of a new pending badge even though nothing is emitted yet', async () => {
    mockedFetchBadgesPage.mockResolvedValueOnce(
      page([badge({ state: 'pending', state_updated_at: null })]),
    )
    const { ctx, emits, saved, deps } = makeCtx()

    await runBadgePoll(ctx)

    expect(emits).toEqual([])
    expect(deps.lockTemplate).toHaveBeenCalledWith('template-1', DEFAULT_TARGET)
    expect(saved.map((row) => [row.badgeId, row.state])).toEqual([['badge-1', 'pending']])
  })

  it('builds a state activity with the member of the first sight, not the live one', async () => {
    mockedFetchBadgesPage.mockResolvedValueOnce(
      page([
        badge({
          state: 'revoked',
          state_updated_at: '2026-10-27T10:00:00.000Z',
          recipient_email: 'new.address@example.org',
          issued_to: 'M. Rossi',
        }),
      ]),
    )
    const { ctx, emitted, saved } = makeCtx(null, () => true, DEFAULT_ROUTING, true, [
      storedBadge({ recipientEmail: 'maria.rossi@example.org', issuedTo: 'Maria Rossi' }),
    ])

    await runBadgePoll(ctx)

    const [activity] = emitted[0] as { member: { identities: { value: string }[] } }[]
    expect(activity.member.identities.map((identity) => identity.value)).toEqual([
      'maria.rossi@example.org',
      'maria.rossi@example.org',
    ])
    expect(saved[0]).toMatchObject({ recipientEmail: 'maria.rossi@example.org', state: 'revoked' })
  })

  it('skips a badge whose stored dates do not parse and keeps the rest of the page', async () => {
    mockedFetchBadgesPage.mockResolvedValueOnce(
      page([
        badge({ id: 'bad-expiry', expires_at: 'not a date' }),
        badge({ id: 'bad-pending', state: 'pending', state_updated_at: null, issued_at: '' }),
        badge({ id: 'good' }),
      ]),
    )
    const { ctx, emitted, saved } = makeCtx()

    await runBadgePoll(ctx)

    expect(emitted).toHaveLength(1)
    expect(saved.map((row) => row.badgeId)).toEqual(['good'])
    expect(LOG.info).toHaveBeenCalledWith(
      expect.objectContaining({ skipped: 2, routed: 1 }),
      expect.any(String),
    )
  })

  it('emits a badge listed twice on one page only once, with its last version', async () => {
    mockedFetchBadgesPage.mockResolvedValueOnce(
      page([badge({ state: 'pending', state_updated_at: null }), badge()]),
    )
    const { ctx, emitted, saved } = makeCtx()

    await runBadgePoll(ctx)

    expect(emitted).toHaveLength(1)
    expect(types(emitted[0])).toEqual([
      CredlyActivityType.ISSUED_BADGE,
      CredlyActivityType.ACCEPTED_BADGE,
    ])
    expect(saved.map((row) => [row.badgeId, row.state])).toEqual([['badge-1', 'accepted']])
  })

  it('neither reads nor writes the state table during a shadow run', async () => {
    mockedFetchBadgesPage.mockResolvedValueOnce(page([badge()]))
    const { ctx, emitted, deps } = makeCtx(null, () => true, DEFAULT_ROUTING, false, [
      storedBadge(),
    ])

    await runBadgePoll(ctx)

    expect(types(emitted[0])).toEqual([
      CredlyActivityType.ISSUED_BADGE,
      CredlyActivityType.ACCEPTED_BADGE,
    ])
    expect(deps.findBadges).not.toHaveBeenCalled()
    expect(deps.saveBadges).not.toHaveBeenCalled()
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

  it('routes mapped templates to their project and holds the rest', async () => {
    const routing: CredlyTemplateRouting = {
      targets: new Map([
        ['tpl-cka', { segmentId: 'k8s', integrationId: 'int-k8s', locked: false }],
        ['tpl-ica', { segmentId: 'istio', integrationId: 'int-istio', locked: true }],
      ]),
      staleTemplateIds: [],
    }
    mockedFetchBadgesPage.mockResolvedValueOnce(
      page([
        badge({ id: 'cka-1', badge_template: { id: 'tpl-cka', name: 'CKA' } }),
        badge({ id: 'speaker-1', badge_template: { id: 'tpl-speaker', name: 'Speaker' } }),
        badge({ id: 'cka-2', badge_template: { id: 'tpl-cka', name: 'CKA' } }),
        badge({ id: 'ica-1', badge_template: { id: 'tpl-ica', name: 'ICA' } }),
      ]),
    )
    const { ctx, emits, deps } = makeCtx(null, () => true, routing)

    await runBadgePoll(ctx)

    const sourceIds = (records: unknown[]) => [
      ...new Set(records.map((record) => (record as { sourceId: string }).sourceId)),
    ]
    expect(
      emits.map(({ records, segmentId, integrationId }) => ({
        ids: sourceIds(records),
        segmentId,
        integrationId,
      })),
    ).toEqual([
      { ids: ['cka-1', 'cka-2'], segmentId: 'k8s', integrationId: 'int-k8s' },
      { ids: ['ica-1'], segmentId: 'istio', integrationId: 'int-istio' },
    ])
    expect(deps.loadTemplateRouting).toHaveBeenCalledTimes(1)
    expect(LOG.info).toHaveBeenCalledWith(
      expect.objectContaining({ event: 'credly_badge_poll_run', routed: 3, held: 1, locked: 1 }),
      expect.any(String),
    )
    expect(LOG.warn).not.toHaveBeenCalled()
  })

  it('locks each emitted template once per run, before its first emit', async () => {
    const routing: CredlyTemplateRouting = {
      targets: new Map([
        ['tpl-cka', { segmentId: 'k8s', integrationId: 'int-k8s', locked: false }],
      ]),
      staleTemplateIds: [],
    }
    mockedFetchBadgesPage
      .mockResolvedValueOnce(
        page([badge({ id: 'cka-1', badge_template: { id: 'tpl-cka', name: 'CKA' } })], 1, 2),
      )
      .mockResolvedValueOnce(
        page([badge({ id: 'cka-2', badge_template: { id: 'tpl-cka', name: 'CKA' } })], 2, 2),
      )
    const { ctx, deps } = makeCtx(null, () => true, routing)
    const order: string[] = []
    ctx.emit = async () => {
      order.push('emit')
    }
    vi.mocked(deps.lockTemplate).mockImplementation(async () => {
      order.push('lock')
      return true
    })

    await runBadgePoll(ctx)

    expect(deps.lockTemplate).toHaveBeenCalledTimes(1)
    expect(deps.lockTemplate).toHaveBeenCalledWith('tpl-cka', {
      segmentId: 'k8s',
      integrationId: 'int-k8s',
      locked: false,
    })
    expect(order).toEqual(['lock', 'emit', 'emit'])
  })

  it.each([
    ['locked first', ['tpl-locked', 'tpl-new']],
    ['locked last', ['tpl-new', 'tpl-locked']],
  ])(
    'locks only the unlocked template of a group sharing one target, %s',
    async (_, templateOrder) => {
      const target = { segmentId: 'k8s', integrationId: 'int-k8s' }
      const routing: CredlyTemplateRouting = {
        targets: new Map([
          ['tpl-locked', { ...target, locked: true }],
          ['tpl-new', { ...target, locked: false }],
        ]),
        staleTemplateIds: [],
      }
      mockedFetchBadgesPage.mockResolvedValueOnce(
        page(templateOrder.map((id) => badge({ id: `b-${id}`, badge_template: { id, name: id } }))),
      )
      const { ctx, emits, deps } = makeCtx(null, () => true, routing)

      await runBadgePoll(ctx)

      expect(emits).toHaveLength(1)
      expect(deps.lockTemplate).toHaveBeenCalledTimes(1)
      expect(deps.lockTemplate).toHaveBeenCalledWith('tpl-new', expect.objectContaining(target))
      expect(LOG.warn).not.toHaveBeenCalled()
    },
  )

  it('keeps the run going when the lock fails or finds the template moved', async () => {
    mockedFetchBadgesPage.mockResolvedValueOnce(
      page([
        badge({ id: 'a', badge_template: { id: 'template-1', name: 'CKA' } }),
        badge({ id: 'b', badge_template: { id: 'tpl-moved', name: 'Moved' } }),
      ]),
    )
    const routing: CredlyTemplateRouting = {
      targets: new Map([
        ['template-1', DEFAULT_TARGET],
        ['tpl-moved', { segmentId: 'k8s', integrationId: 'int-k8s', locked: false }],
      ]),
      staleTemplateIds: [],
    }
    const { ctx, emits, deps } = makeCtx(null, () => true, routing)
    vi.mocked(deps.lockTemplate).mockImplementation(async (templateId) => {
      if (templateId === 'template-1') throw new Error('connection reset')
      return false
    })

    const outcome = await runBadgePoll(ctx)

    expect(outcome).toEqual({ complete: true })
    expect(emits).toHaveLength(2)
    expect(LOG.warn).toHaveBeenCalledWith(
      { badgeTemplateId: 'template-1', err: expect.any(Error) },
      'credly template lock failed',
    )
    expect(LOG.warn).toHaveBeenCalledWith(
      { badgeTemplateId: 'tpl-moved' },
      'credly template moved during the run',
    )
  })

  it('never locks a template during a shadow run', async () => {
    mockedFetchBadgesPage.mockResolvedValueOnce(page([badge()]))
    const { ctx, emits, deps } = makeCtx(null, () => true, DEFAULT_ROUTING, false)

    await runBadgePoll(ctx)

    expect(emits).toHaveLength(1)
    expect(deps.lockTemplate).not.toHaveBeenCalled()
    expect(LOG.info).toHaveBeenCalledWith(
      expect.objectContaining({ event: 'credly_badge_poll_run', locked: 0 }),
      expect.any(String),
    )
  })

  it('warns once per run about mappings whose integration is gone, and holds their badges', async () => {
    const routing: CredlyTemplateRouting = { targets: new Map(), staleTemplateIds: ['tpl-old'] }
    mockedFetchBadgesPage.mockResolvedValueOnce(
      page([badge({ id: 'old-1', badge_template: { id: 'tpl-old', name: 'Old' } })]),
    )
    const { ctx, emits, deps } = makeCtx(null, () => true, routing)

    await runBadgePoll(ctx)

    expect(emits).toEqual([])
    expect(deps.lockTemplate).not.toHaveBeenCalled()
    expect(LOG.warn).toHaveBeenCalledWith(
      { badgeTemplateIds: ['tpl-old'] },
      'credly template mappings point to a deleted or moved integration, their badges are held',
    )
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

describe('runBadgePoll replacements', () => {
  const RENEWAL_AT = '2026-10-22T11:00:00.000Z'

  function replaced(id: string, replacementBadgeId: string, overrides: Partial<CredlyBadge> = {}) {
    return badge({
      id,
      state: 'replaced',
      replacement_badge_id: replacementBadgeId,
      badge_url: null,
      ...overrides,
    })
  }

  function renewed(id: string, replacesBadgeId: string, overrides: Partial<CredlyBadge> = {}) {
    return badge({
      id,
      issued_at: '2020-02-10T00:00:00.000Z',
      state_updated_at: '2020-02-10T09:00:00.000Z',
      created_at: RENEWAL_AT,
      replaces_badge_id: replacesBadgeId,
      badge_url: null,
      ...overrides,
    })
  }

  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(NOW)
    mockedFetchBadgesPage.mockReset()
    mockedFetchBadge.mockReset()
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.clearAllMocks()
  })

  it('fetches the renewed badge of a replaced one and emits it dated at the renewal', async () => {
    mockedFetchBadgesPage.mockResolvedValueOnce(page([replaced('old', 'new')]))
    mockedFetchBadge.mockResolvedValueOnce(renewed('new', 'old'))
    const { ctx, emits, deps, saved } = makeCtx()

    const outcome = await runBadgePoll(ctx)

    expect(outcome).toEqual({ complete: true })
    expect(mockedFetchBadge).toHaveBeenCalledWith(
      ctx.http,
      { organizationId: ORGANIZATION_ID, badgeId: 'new' },
      LOG,
    )
    expect(deps.findBadges).toHaveBeenNthCalledWith(1, ['new'])
    expect(emits).toHaveLength(1)
    expect(
      emits[0].records.map((r) => {
        const { sourceId, type, timestamp } = r as Record<string, string>
        return [sourceId, type, timestamp]
      }),
    ).toEqual([
      ['old', CredlyActivityType.ISSUED_BADGE, '2026-10-22T00:00:00.000Z'],
      ['old', CredlyActivityType.REPLACED_BADGE, '2026-10-22T10:00:00.000Z'],
      ['new', CredlyActivityType.ISSUED_BADGE, RENEWAL_AT],
      ['new', CredlyActivityType.ACCEPTED_BADGE, RENEWAL_AT],
    ])
    expect(saved.map((row) => [row.badgeId, row.state, row.replacesBadgeId])).toEqual([
      ['old', 'replaced', null],
      ['new', 'accepted', 'old'],
    ])
    expect(LOG.info).toHaveBeenCalledWith(
      expect.objectContaining({ event: 'credly_badge_poll_run', replacements: 1 }),
      expect.any(String),
    )
  })

  it('follows a chain of replacements to its end, each badge once', async () => {
    mockedFetchBadgesPage.mockResolvedValueOnce(page([replaced('a', 'b')]))
    mockedFetchBadge
      .mockResolvedValueOnce(renewed('b', 'a', { state: 'replaced', replacement_badge_id: 'c' }))
      .mockResolvedValueOnce(renewed('c', 'b'))
    const { ctx, emitted } = makeCtx()

    await runBadgePoll(ctx)

    expect(mockedFetchBadge.mock.calls.map(([, params]) => params.badgeId)).toEqual(['b', 'c'])
    expect(emitted[0].map((r) => (r as { sourceId: string }).sourceId)).toEqual([
      'a',
      'a',
      'b',
      'b',
      'c',
      'c',
    ])
  })

  it('does not fetch a replacement the state table knows or the page already lists', async () => {
    mockedFetchBadgesPage.mockResolvedValueOnce(
      page([replaced('old-1', 'known'), replaced('old-2', 'listed'), renewed('listed', 'old-2')]),
    )
    const { ctx, deps } = makeCtx(null, () => true, DEFAULT_ROUTING, true, [
      storedBadge({ badgeId: 'known' }),
    ])

    await runBadgePoll(ctx)

    expect(deps.findBadges).toHaveBeenNthCalledWith(1, ['known'])
    expect(mockedFetchBadge).not.toHaveBeenCalled()
  })

  it('follows the link of a replaced badge whatever its state is now', async () => {
    mockedFetchBadgesPage.mockResolvedValueOnce(
      page([replaced('old', 'new', { state: 'revoked', revocation_reason: 'Issued twice' })]),
    )
    mockedFetchBadge.mockResolvedValueOnce(renewed('new', 'old'))
    const { ctx, emitted } = makeCtx()

    await runBadgePoll(ctx)

    expect(emitted[0].map((r) => (r as { sourceId: string }).sourceId)).toEqual([
      'old',
      'old',
      'new',
      'new',
    ])
  })

  it('warns when a chain is longer than the hop cap', async () => {
    mockedFetchBadgesPage.mockResolvedValueOnce(page([replaced('b-0', 'b-1')]))
    for (let i = 1; i <= MAX_REPLACEMENT_HOPS; i += 1) {
      mockedFetchBadge.mockResolvedValueOnce(
        renewed(`b-${i}`, `b-${i - 1}`, { state: 'replaced', replacement_badge_id: `b-${i + 1}` }),
      )
    }
    const { ctx } = makeCtx()

    await runBadgePoll(ctx)

    expect(mockedFetchBadge).toHaveBeenCalledTimes(MAX_REPLACEMENT_HOPS)
    expect(LOG.warn).toHaveBeenCalledWith(
      { badgeIds: [`b-${MAX_REPLACEMENT_HOPS + 1}`] },
      'credly replacement chain cut at the hop cap',
    )
  })

  it('follows the stored link of a known replacement to reach the end of a chain', async () => {
    mockedFetchBadgesPage.mockResolvedValueOnce(page([replaced('a', 'b')]))
    mockedFetchBadge.mockResolvedValueOnce(renewed('c', 'b'))
    const { ctx, deps, emitted } = makeCtx(null, () => true, DEFAULT_ROUTING, true, [
      storedBadge({ badgeId: 'b', state: 'replaced', replacementBadgeId: 'c' }),
    ])

    await runBadgePoll(ctx)

    expect(vi.mocked(deps.findBadges).mock.calls.slice(0, 2)).toEqual([[['b']], [['c']]])
    expect(mockedFetchBadge.mock.calls.map(([, params]) => params.badgeId)).toEqual(['c'])
    expect(emitted[0].map((r) => (r as { sourceId: string }).sourceId)).toEqual([
      'a',
      'a',
      'c',
      'c',
    ])
  })

  it('leaves the replacements of held templates alone', async () => {
    mockedFetchBadgesPage.mockResolvedValueOnce(
      page([replaced('old', 'new', { badge_template: { id: 'tpl-held', name: 'Held' } })]),
    )
    const { ctx } = makeCtx()

    await runBadgePoll(ctx)

    expect(mockedFetchBadge).not.toHaveBeenCalled()
  })

  it('skips a replacement credly no longer serves and keeps the rest', async () => {
    mockedFetchBadgesPage.mockResolvedValueOnce(
      page([replaced('old-1', 'gone'), replaced('old-2', 'new')]),
    )
    mockedFetchBadge
      .mockRejectedValueOnce(
        new ProviderContractError('provider returned status 404', { status: 404 }),
      )
      .mockResolvedValueOnce(renewed('new', 'old-2'))
    const { ctx, emitted } = makeCtx()

    await runBadgePoll(ctx)

    expect(emitted[0].map((r) => (r as { sourceId: string }).sourceId)).toEqual([
      'old-1',
      'old-1',
      'old-2',
      'old-2',
      'new',
      'new',
    ])
    expect(LOG.warn).toHaveBeenCalledWith(
      { badgeId: 'gone' },
      'credly replacement badge not found, skipping it',
    )
    expect(LOG.info).toHaveBeenCalledWith(
      expect.objectContaining({ replacements: 1, replacementsMissing: 1 }),
      expect.any(String),
    )
  })

  it('fails the run on any other error from the replacement fetch', async () => {
    mockedFetchBadgesPage.mockResolvedValueOnce(page([replaced('old', 'new')]))
    mockedFetchBadge.mockRejectedValueOnce(
      new ProviderUnavailableError('provider returned status 503', { status: 503 }),
    )
    const { ctx, emitted } = makeCtx()

    await expect(runBadgePoll(ctx)).rejects.toBeInstanceOf(ProviderUnavailableError)
    expect(emitted).toHaveLength(0)
  })

  it('defers the whole page when the budget runs out while following links', async () => {
    const since = '2026-10-21T00:00:00.000Z'
    mockedFetchBadgesPage.mockResolvedValueOnce(page([replaced('old', 'new')]))
    let calls = 0
    const { ctx, emitted, lastWatermark } = makeCtx(
      { since, until: HORIZON, page: 1 },
      () => calls++ < 1,
    )

    const outcome = await runBadgePoll(ctx)

    expect(outcome).toEqual({ complete: false })
    expect(mockedFetchBadge).not.toHaveBeenCalled()
    expect(emitted).toHaveLength(0)
    expect(lastWatermark()).toBeUndefined()
  })

  it('fetches every replacement during a shadow run without reading the state table', async () => {
    mockedFetchBadgesPage.mockResolvedValueOnce(page([replaced('old', 'new')]))
    mockedFetchBadge.mockResolvedValueOnce(renewed('new', 'old'))
    const { ctx, emitted, deps } = makeCtx(null, () => true, DEFAULT_ROUTING, false, [
      storedBadge({ badgeId: 'new' }),
    ])

    await runBadgePoll(ctx)

    expect(deps.findBadges).not.toHaveBeenCalled()
    expect(mockedFetchBadge).toHaveBeenCalledTimes(1)
    expect(emitted[0]).toHaveLength(4)
  })
})

describe('runBadgePoll backfill', () => {
  const FLOOR = '2026-10-10T00:00:00.000Z'
  const TOP = '2026-10-20T00:00:00.000Z'
  const MID = new Date(Date.parse(TOP) - MAX_WINDOW_MS).toISOString()

  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(NOW)
    mockedFetchBadgesPage.mockReset()
    mockedFetchBadge.mockReset()
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.clearAllMocks()
  })

  it('walks one window back from the top once the live cursor is caught up, and stays incomplete', async () => {
    mockedFetchBadgesPage.mockResolvedValueOnce(page([badge({ id: 'old-1' })]))
    let budgetChecks = 0
    const { ctx, emitted, lastWatermark } = makeCtx(
      { since: HORIZON, backfill: { floor: FLOOR, top: TOP } },
      () => budgetChecks++ < 2,
    )

    const outcome = await runBadgePoll(ctx)

    expect(outcome).toEqual({ complete: false })
    expect(requestedParams()).toEqual([
      { organizationId: ORGANIZATION_ID, window: { min: MID, max: TOP }, page: 1 },
    ])
    expect(emitted).toHaveLength(1)
    expect(lastWatermark()).toEqual({
      since: HORIZON,
      until: null,
      page: 1,
      backfill: { floor: FLOOR, top: MID, since: null, until: null, page: 1 },
    })
    expect(LOG.info).toHaveBeenCalledWith(
      expect.objectContaining({
        event: 'credly_badge_poll_run',
        backfillPages: 1,
        backfillWindows: 1,
      }),
      expect.any(String),
    )
  })

  it('clips the last window at the floor, then drops the cursor and completes', async () => {
    mockedFetchBadgesPage.mockResolvedValue(page([]))
    const { ctx, lastWatermark } = makeCtx({
      since: HORIZON,
      backfill: { floor: FLOOR, top: TOP },
    })

    const outcome = await runBadgePoll(ctx)

    expect(outcome).toEqual({ complete: true })
    expect(requestedParams().map((p) => p.window)).toEqual([
      { min: MID, max: TOP },
      { min: FLOOR, max: MID },
    ])
    expect(lastWatermark()).toEqual({ since: HORIZON, until: null, page: 1, historyFrom: FLOOR })
    expect(LOG.info).toHaveBeenCalledWith({ floor: FLOOR }, 'credly backfill reached its floor')
  })

  it('keeps the lowest floor reached across armings', async () => {
    mockedFetchBadgesPage.mockResolvedValue(page([]))
    const earlier = '2026-10-01T00:00:00.000Z'
    const { ctx, lastWatermark } = makeCtx({
      since: HORIZON,
      historyFrom: earlier,
      backfill: { floor: FLOOR, top: TOP },
    })

    await runBadgePoll(ctx)

    expect(lastWatermark()).toEqual({ since: HORIZON, until: null, page: 1, historyFrom: earlier })
  })

  it('serves the live window first and keeps the backfill cursor through it', async () => {
    const since = '2026-10-23T00:00:00.000Z'
    mockedFetchBadgesPage.mockResolvedValue(page([]))
    const { ctx, watermarks } = makeCtx({ since, backfill: { floor: FLOOR, top: TOP } })

    await runBadgePoll(ctx)

    expect(requestedParams().map((p) => p.window)).toEqual([
      { min: since, max: HORIZON },
      { min: MID, max: TOP },
      { min: FLOOR, max: MID },
    ])
    expect(watermarks[0]).toEqual({
      since,
      until: HORIZON,
      page: 1,
      backfill: { floor: FLOOR, top: TOP, since: null, until: null, page: 1 },
    })
  })

  it('resumes a cut backfill window from its saved page', async () => {
    mockedFetchBadgesPage.mockResolvedValue(page([], 3, 3))
    const { ctx, lastWatermark } = makeCtx({
      since: HORIZON,
      backfill: { floor: FLOOR, top: TOP, since: MID, until: TOP, page: 3 },
    })

    await runBadgePoll(ctx)

    expect(requestedParams()[0]).toEqual({
      organizationId: ORGANIZATION_ID,
      window: { min: MID, max: TOP },
      page: 3,
    })
    expect(lastWatermark()).toEqual({ since: HORIZON, until: null, page: 1, historyFrom: FLOOR })
  })

  it('halves a failing backfill window and walks its upper half before moving down', async () => {
    const err = new ProviderUnavailableError('provider returned status 500', { status: 500 })
    mockedFetchBadgesPage.mockRejectedValueOnce(err).mockResolvedValue(page([]))
    const { ctx } = makeCtx({ since: HORIZON, backfill: { floor: FLOOR, top: TOP } })

    await runBadgePoll(ctx)

    const half = new Date(Date.parse(MID) + MAX_WINDOW_MS / 2).toISOString()
    expect(requestedParams().map((p) => p.window)).toEqual([
      { min: MID, max: TOP },
      { min: MID, max: half },
      { min: half, max: TOP },
      { min: FLOOR, max: MID },
    ])
  })

  it('pauses at the page cap with the backfill position saved', async () => {
    mockedFetchBadgesPage.mockResolvedValue(
      page([badge({ id: 'b', state_updated_at: MID })], 1, 1000),
    )
    const { ctx, lastWatermark } = makeCtx({
      since: HORIZON,
      backfill: { floor: FLOOR, top: TOP },
    })

    const outcome = await runBadgePoll(ctx)

    expect(outcome).toEqual({ complete: false })
    expect(mockedFetchBadgesPage).toHaveBeenCalledTimes(MAX_PAGES_PER_RUN)
    expect(lastWatermark()).toEqual({
      since: HORIZON,
      until: null,
      page: 1,
      backfill: { floor: FLOOR, top: TOP, since: MID, until: TOP, page: MAX_PAGES_PER_RUN + 1 },
    })
  })

  it('drops a backfill cursor it cannot read and commits the watermark without it', async () => {
    const { ctx, watermarks } = makeCtx({
      since: HORIZON,
      backfill: { floor: FLOOR, top: 'not a date' },
    })

    const outcome = await runBadgePoll(ctx)

    expect(outcome).toEqual({ complete: true })
    expect(mockedFetchBadgesPage).not.toHaveBeenCalled()
    expect(watermarks).toEqual([{ since: HORIZON, until: null, page: 1 }])
    expect(LOG.warn).toHaveBeenCalledWith(
      { backfill: { floor: FLOOR, top: 'not a date' } },
      'credly backfill cursor unreadable, dropped',
    )
  })
})
