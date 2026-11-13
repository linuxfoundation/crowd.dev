import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import * as activities from '../activities'
import { createDownloadsThrottle } from '../downloadsThrottle'
import { type FetchError, FetchErrorKind } from '../types'

const h = vi.hoisted(() => ({
  throttle: { current: undefined as unknown },
  ctx: {
    heartbeat: vi.fn(),
    info: { attempt: 1, scheduledTimestampMs: 0 },
    cancellationSignal: undefined as AbortSignal | undefined,
  },
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
  dal: {
    getExistingLast30dEndDates: vi.fn(),
    getMissingDownloadDates: vi.fn(),
    getNewestDownloadDate: vi.fn(),
    getNpmChangesLastSeq: vi.fn(),
    getNpmPackagesNeedingDailyBackfill: vi.fn(),
    getNpmPurlsDueForLast30dHistory: vi.fn(),
    getNpmPurlsDueForLatest30d: vi.fn(),
    getNpmPurlsForChangedNames: vi.fn(),
    getNpmPurlsScannedSince: vi.fn(),
    getUnscannedNpmPurls: vi.fn(),
    logAuditFieldChanges: vi.fn(),
    markDailyDownloadsProcessed: vi.fn(),
    markLast30dHistoryBackfilled: vi.fn(),
    markLast30dProcessed: vi.fn(),
    markNpmPackageScanned: vi.fn(),
    setNpmChangesLastSeq: vi.fn(),
    upsertDailyDownloads: vi.fn(),
    upsertLast30dDownload: vi.fn(),
  },
  fetch: {
    fetchBulkDailyRange: vi.fn(),
    fetchBulkPointRange: vi.fn(),
    fetchDailyRange: vi.fn(),
    fetchLastPublishedDay: vi.fn(),
    fetchPointRange: vi.fn(),
  },
}))

vi.mock('@temporalio/activity', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@temporalio/activity')>()),
  Context: { current: () => h.ctx },
}))
vi.mock('@crowd/data-access-layer/src/packages', () => h.dal)
vi.mock('@crowd/logging', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/logging')>()),
  getServiceChildLogger: () => h.log,
}))
vi.mock('../../db', () => ({ getPackagesDb: vi.fn(async () => ({})) }))
vi.mock('../fetchDownloads', () => h.fetch)
// The limiter is process-wide state, so each test gets a fresh one through the accessor.
vi.mock('../downloadsThrottle', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../downloadsThrottle')>()),
  npmDownloadsThrottle: () => h.throttle.current,
}))

const MIN = 60_000
const CUTOFF = '2026-11-13T03:30:00.000Z'
// npm's latest published day while the run starts at 04:00 UTC on Nov 13 (yesterday is Nov 12).
const UPPER = '2026-11-11'

const rateLimited: FetchError = {
  kind: FetchErrorKind.RATE_LIMIT,
  statusCode: 429,
  message: 'rate limited by npm downloads API',
}

function addDays(day: string, n: number): string {
  const d = new Date(`${day}T00:00:00Z`)
  d.setUTCDate(d.getUTCDate() + n)
  return d.toISOString().slice(0, 10)
}

function days(start: string, end: string): string[] {
  const out: string[] = []
  for (let d = start; d <= end; d = addDays(d, 1)) out.push(d)
  return out
}

function rows(start: string, end: string, count = 5) {
  return days(start, end).map((day) => ({ day, downloads: count }))
}

function scoped(name: string, firstReleaseAt = '2020-01-01') {
  return {
    id: `id-${name}`,
    purl: `pkg:npm/%40scope/${name}`,
    name: `@scope/${name}`,
    firstReleaseAt,
  }
}

function unscoped(name: string, firstReleaseAt = '2020-01-01') {
  return { id: `id-${name}`, purl: `pkg:npm/${name}`, name, firstReleaseAt }
}

// Per-package DB state: the newest stored day and the missing days up to the bound.
function storedState(state: Record<string, { newest: string | null; missing: string[] }>): void {
  h.dal.getNewestDownloadDate.mockImplementation(
    async (_qx: unknown, id: string) => state[id]?.newest ?? null,
  )
  h.dal.getMissingDownloadDates.mockImplementation(
    async (_qx: unknown, id: string) => state[id]?.missing ?? [],
  )
}

function upsertedDays(id: string): string[] {
  return h.dal.upsertDailyDownloads.mock.calls
    .filter((c) => c[1] === id)
    .flatMap((c) => (c[2] as Array<{ day: string }>).map((r) => r.day))
}

function markedWith(purl: string) {
  return h.dal.markDailyDownloadsProcessed.mock.calls.filter((c) => c[1] === purl).map((c) => c[2])
}

async function settle<T>(promise: Promise<T>, advanceMs: number) {
  const outcome = promise.then(
    (value) => ({ value, error: undefined }),
    (error: unknown) => ({ value: undefined, error }),
  )
  await vi.advanceTimersByTimeAsync(advanceMs)
  return outcome
}

beforeEach(() => {
  vi.useFakeTimers()
  h.throttle.current = createDownloadsThrottle(1000, { info: vi.fn(), warn: vi.fn() })
  h.ctx.cancellationSignal = undefined
  vi.setSystemTime(new Date('2026-11-13T04:00:00Z'))
  vi.stubEnv('CROWD_PACKAGES_PROXIES_ENABLED', 'false')
  for (const fn of [
    ...Object.values(h.dal),
    ...Object.values(h.fetch),
    ...Object.values(h.log),
    h.ctx.heartbeat,
  ]) {
    fn.mockReset()
  }
  storedState({})
  h.dal.upsertDailyDownloads.mockResolvedValue({ inserted: 0, overwritten: 0 })
  h.fetch.fetchDailyRange.mockImplementation(async (_name: string, start: string, end: string) => ({
    start,
    end,
    downloads: rows(start, end),
  }))
  h.fetch.fetchBulkDailyRange.mockImplementation(
    async (names: string[], start: string, end: string) => ({
      downloads: new Map(names.map((n) => [n, rows(start, end)])),
    }),
  )
})

afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllEnvs()
})

describe('getNpmLastPublishedDay', () => {
  it('returns the latest day npm has published', async () => {
    h.fetch.fetchLastPublishedDay.mockResolvedValue({ day: UPPER })

    const { value, error } = await settle(activities.getNpmLastPublishedDay(), MIN)

    expect(error).toBeUndefined()
    expect(value).toEqual({ day: UPPER })
    expect(h.fetch.fetchLastPublishedDay).toHaveBeenCalledTimes(1)
  })

  it('waits out a 429 in the shared limiter and asks again', async () => {
    h.fetch.fetchLastPublishedDay
      .mockResolvedValueOnce(rateLimited)
      .mockResolvedValueOnce({ day: UPPER })

    const { value, error } = await settle(activities.getNpmLastPublishedDay(), 2 * MIN)

    expect(error).toBeUndefined()
    expect(value).toEqual({ day: UPPER })
    expect(h.fetch.fetchLastPublishedDay).toHaveBeenCalledTimes(2)
  })

  it('reports blocked after a long block', async () => {
    h.fetch.fetchLastPublishedDay.mockResolvedValue(rateLimited)

    const { value, error } = await settle(activities.getNpmLastPublishedDay(), 45 * MIN)

    expect(error).toBeUndefined()
    expect(value).toEqual({ blocked: true })
  })

  it('throws on a 5xx so Temporal retries the activity', async () => {
    h.fetch.fetchLastPublishedDay.mockResolvedValue({
      kind: FetchErrorKind.TRANSIENT,
      statusCode: 503,
      message: 'HTTP 503',
    })

    const { error } = await settle(activities.getNpmLastPublishedDay(), MIN)

    expect(error).toBeDefined()
  })
})

describe('backfillDailyLane: published-day bound', () => {
  it('never asks for a day after the published bound (regression)', async () => {
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([scoped('a')])
    storedState({ 'id-a': { newest: '2026-11-09', missing: ['2026-11-10', '2026-11-11'] } })

    const { error } = await settle(activities.backfillDailyLane(CUTOFF, 128, 0, 1, UPPER), MIN)

    expect(error).toBeUndefined()
    expect(h.dal.getMissingDownloadDates).toHaveBeenCalledWith(
      expect.anything(),
      'id-a',
      '2020-01-01',
      UPPER,
    )
    expect(h.dal.getNewestDownloadDate).toHaveBeenCalledWith(expect.anything(), 'id-a', UPPER)
    expect(h.fetch.fetchDailyRange).toHaveBeenCalledTimes(1)
    for (const call of h.fetch.fetchDailyRange.mock.calls) expect(call[2] <= UPPER).toBe(true)
  })
})

describe('backfillDailyLane: trailing overwrite', () => {
  it('refetches a zero stored on the newest day and hands npm count to the upsert (regression)', async () => {
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([scoped('a')])
    storedState({ 'id-a': { newest: '2026-11-09', missing: ['2026-11-10', '2026-11-11'] } })
    h.fetch.fetchDailyRange.mockResolvedValue({
      start: '2026-11-03',
      end: UPPER,
      downloads: rows('2026-11-03', UPPER).map((r) =>
        r.day === '2026-11-09' ? { ...r, downloads: 120 } : r,
      ),
    })

    const { error } = await settle(activities.backfillDailyLane(CUTOFF, 128, 0, 1, UPPER), MIN)

    expect(error).toBeUndefined()
    expect(h.fetch.fetchDailyRange).toHaveBeenCalledTimes(1)
    expect(h.fetch.fetchDailyRange.mock.calls[0].slice(0, 3)).toEqual([
      '@scope/a',
      '2026-11-03',
      UPPER,
    ])
    expect(h.dal.upsertDailyDownloads).toHaveBeenCalledWith(
      expect.anything(),
      'id-a',
      expect.arrayContaining([{ day: '2026-11-09', downloads: 120 }]),
    )
    expect(markedWith('pkg:npm/%40scope/a')).toEqual([{ status: 'success' }])
  })

  it('refetches the 7 days ending at the newest stored day when nothing is missing', async () => {
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([scoped('a')])
    storedState({ 'id-a': { newest: UPPER, missing: [] } })

    const { error } = await settle(activities.backfillDailyLane(CUTOFF, 128, 0, 1, UPPER), MIN)

    expect(error).toBeUndefined()
    expect(h.fetch.fetchDailyRange).toHaveBeenCalledTimes(1)
    expect(h.fetch.fetchDailyRange.mock.calls[0].slice(0, 3)).toEqual([
      '@scope/a',
      '2026-11-05',
      UPPER,
    ])
  })

  it('starts at the first release when it falls inside the trailing window', async () => {
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([scoped('a', '2026-11-08')])
    storedState({ 'id-a': { newest: '2026-11-09', missing: ['2026-11-10', '2026-11-11'] } })

    const { error } = await settle(activities.backfillDailyLane(CUTOFF, 128, 0, 1, UPPER), MIN)

    expect(error).toBeUndefined()
    expect(h.fetch.fetchDailyRange.mock.calls[0].slice(0, 3)).toEqual([
      '@scope/a',
      '2026-11-08',
      UPPER,
    ])
  })

  it('fetches only the missing days when nothing is stored yet', async () => {
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([scoped('a', '2026-11-01')])
    storedState({ 'id-a': { newest: null, missing: days('2026-11-01', UPPER) } })

    const { error } = await settle(activities.backfillDailyLane(CUTOFF, 128, 0, 1, UPPER), MIN)

    expect(error).toBeUndefined()
    expect(h.fetch.fetchDailyRange.mock.calls[0].slice(0, 3)).toEqual([
      '@scope/a',
      '2026-11-01',
      UPPER,
    ])
  })

  it('logs the rows the lane inserted and overwrote', async () => {
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([scoped('a'), scoped('b')])
    storedState({
      'id-a': { newest: '2026-11-10', missing: [UPPER] },
      'id-b': { newest: '2026-11-10', missing: [UPPER] },
    })
    h.dal.upsertDailyDownloads.mockResolvedValue({ inserted: 2, overwritten: 1 })

    const { error } = await settle(activities.backfillDailyLane(CUTOFF, 128, 0, 1, UPPER), MIN)

    expect(error).toBeUndefined()
    expect(h.log.info).toHaveBeenCalledWith(
      expect.objectContaining({ inserted: 4, overwritten: 2 }),
      'Backfilled daily downloads lane',
    )
  })
})

describe('backfillDailyLane: fewest requests', () => {
  it('covers scattered missing days in one request', async () => {
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([scoped('a')])
    storedState({
      'id-a': { newest: UPPER, missing: ['2026-03-01', '2026-06-15', '2026-09-30'] },
    })

    const { error } = await settle(activities.backfillDailyLane(CUTOFF, 128, 0, 1, UPPER), MIN)

    expect(error).toBeUndefined()
    expect(h.fetch.fetchDailyRange).toHaveBeenCalledTimes(1)
    expect(h.fetch.fetchDailyRange.mock.calls[0].slice(0, 3)).toEqual([
      '@scope/a',
      '2026-03-01',
      UPPER,
    ])
  })

  it('splits a span longer than 540 days into two requests', async () => {
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([scoped('a', '2024-01-01')])
    storedState({ 'id-a': { newest: null, missing: days('2024-01-01', UPPER) } })

    const { error } = await settle(activities.backfillDailyLane(CUTOFF, 128, 0, 1, UPPER), MIN)

    expect(error).toBeUndefined()
    expect(h.fetch.fetchDailyRange.mock.calls.map((c) => c.slice(0, 3))).toEqual([
      ['@scope/a', '2024-01-01', addDays('2024-01-01', 539)],
      ['@scope/a', addDays('2024-01-01', 540), UPPER],
    ])
    expect(markedWith('pkg:npm/%40scope/a')).toEqual([{ status: 'success' }])
  })
})

describe('backfillDailyLane: bulk for unscoped packages', () => {
  it('fetches unscoped packages in one bulk request', async () => {
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([
      unscoped('a'),
      unscoped('b'),
      unscoped('c'),
    ])
    const state = { newest: '2026-11-09', missing: ['2026-11-10', UPPER] }
    storedState({ 'id-a': state, 'id-b': state, 'id-c': state })

    const { error } = await settle(activities.backfillDailyLane(CUTOFF, 128, 0, 1, UPPER), MIN)

    expect(error).toBeUndefined()
    expect(h.fetch.fetchBulkDailyRange).toHaveBeenCalledTimes(1)
    expect(h.fetch.fetchBulkDailyRange.mock.calls[0].slice(0, 3)).toEqual([
      ['a', 'b', 'c'],
      '2026-11-03',
      UPPER,
    ])
    expect(h.fetch.fetchDailyRange).not.toHaveBeenCalled()
    for (const id of ['id-a', 'id-b', 'id-c']) {
      expect(upsertedDays(id)).toEqual(days('2026-11-03', UPPER))
    }
    for (const purl of ['pkg:npm/a', 'pkg:npm/b', 'pkg:npm/c']) {
      expect(markedWith(purl)).toEqual([{ status: 'success' }])
    }
  })

  it('starts the bulk request at the earliest day any package in the batch needs', async () => {
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([unscoped('a'), unscoped('b')])
    storedState({
      'id-a': { newest: '2026-11-09', missing: ['2026-11-10', UPPER] },
      'id-b': { newest: '2026-11-01', missing: days('2026-11-02', UPPER) },
    })

    const { error } = await settle(activities.backfillDailyLane(CUTOFF, 128, 0, 1, UPPER), MIN)

    expect(error).toBeUndefined()
    expect(h.fetch.fetchBulkDailyRange.mock.calls[0].slice(1, 3)).toEqual(['2026-10-26', UPPER])
  })

  it('splits more than 128 unscoped packages across bulk requests', async () => {
    const names = Array.from({ length: 130 }, (_, i) => `p${i}`)
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue(names.map((n) => unscoped(n)))
    storedState(
      Object.fromEntries(names.map((n) => [`id-${n}`, { newest: '2026-11-10', missing: [UPPER] }])),
    )

    const { error } = await settle(activities.backfillDailyLane(CUTOFF, 130, 0, 1, UPPER), MIN)

    expect(error).toBeUndefined()
    expect(h.fetch.fetchBulkDailyRange.mock.calls.map((c) => (c[0] as string[]).length)).toEqual([
      128, 2,
    ])
  })

  it('sends a lone unscoped package through the single-package endpoint', async () => {
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([unscoped('a'), scoped('b')])
    storedState({
      'id-a': { newest: '2026-11-09', missing: ['2026-11-10', UPPER] },
      'id-b': { newest: '2026-11-09', missing: ['2026-11-10', UPPER] },
    })

    const { error } = await settle(activities.backfillDailyLane(CUTOFF, 128, 0, 1, UPPER), MIN)

    expect(error).toBeUndefined()
    expect(h.fetch.fetchBulkDailyRange).not.toHaveBeenCalled()
    expect(h.fetch.fetchDailyRange.mock.calls.map((c) => c.slice(0, 3))).toEqual([
      ['a', '2026-11-03', UPPER],
      ['@scope/b', '2026-11-03', UPPER],
    ])
  })

  it('marks a package missing from the bulk response as processed with an error', async () => {
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([unscoped('a'), unscoped('gone')])
    const state = { newest: '2026-11-10', missing: [UPPER] }
    storedState({ 'id-a': state, 'id-gone': state })
    h.fetch.fetchBulkDailyRange.mockResolvedValue({
      downloads: new Map([['a', rows('2026-11-04', UPPER)]]),
    })

    const { error } = await settle(activities.backfillDailyLane(CUTOFF, 128, 0, 1, UPPER), MIN)

    expect(error).toBeUndefined()
    expect(markedWith('pkg:npm/a')).toEqual([{ status: 'success' }])
    expect(markedWith('pkg:npm/gone')).toEqual([expect.objectContaining({ status: 'error' })])
    expect(upsertedDays('id-gone')).toEqual([])
    expect(h.fetch.fetchDailyRange).not.toHaveBeenCalled()
  })

  it('fetches needs older than 365 days through single-package ranges', async () => {
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([
      unscoped('a'),
      unscoped('b', '2024-06-01'),
    ])
    storedState({
      'id-a': { newest: '2026-11-10', missing: [UPPER] },
      'id-b': { newest: null, missing: days('2024-06-01', UPPER) },
    })
    const floor = addDays(UPPER, -364)

    const { error } = await settle(activities.backfillDailyLane(CUTOFF, 128, 0, 1, UPPER), MIN)

    expect(error).toBeUndefined()
    expect(h.fetch.fetchBulkDailyRange.mock.calls[0].slice(0, 3)).toEqual([
      ['a', 'b'],
      floor,
      UPPER,
    ])
    expect(h.fetch.fetchDailyRange.mock.calls.map((c) => c.slice(0, 3))).toEqual([
      ['b', '2024-06-01', addDays(floor, -1)],
    ])
    expect(upsertedDays('id-b')).toEqual(
      expect.arrayContaining(['2024-06-01', addDays(floor, -1), floor, UPPER]),
    )
    expect(markedWith('pkg:npm/b')).toEqual([{ status: 'success' }])
  })

  it('drops bulk rows dated before a package first release', async () => {
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([
      unscoped('a'),
      unscoped('b', '2026-11-07'),
    ])
    storedState({
      'id-a': { newest: null, missing: days('2026-11-01', UPPER) },
      'id-b': { newest: null, missing: days('2026-11-07', UPPER) },
    })

    const { error } = await settle(activities.backfillDailyLane(CUTOFF, 128, 0, 1, UPPER), MIN)

    expect(error).toBeUndefined()
    expect(h.fetch.fetchBulkDailyRange.mock.calls[0].slice(1, 3)).toEqual(['2026-11-01', UPPER])
    expect(upsertedDays('id-b')).toEqual(days('2026-11-07', UPPER))
  })

  it('returns blocked on a long block during the bulk request without marking anything', async () => {
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([unscoped('a'), unscoped('b')])
    const state = { newest: '2026-11-10', missing: [UPPER] }
    storedState({ 'id-a': state, 'id-b': state })
    h.fetch.fetchBulkDailyRange.mockResolvedValue(rateLimited)

    const { value, error } = await settle(
      activities.backfillDailyLane(CUTOFF, 128, 0, 1, UPPER),
      30 * MIN,
    )

    expect(error).toBeUndefined()
    expect(value).toEqual({ fetched: 2, blocked: true })
    expect(h.dal.upsertDailyDownloads).not.toHaveBeenCalled()
    expect(h.dal.markDailyDownloadsProcessed).not.toHaveBeenCalled()
  })

  it('throws on a bulk 5xx so Temporal retries the lane', async () => {
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([unscoped('a'), unscoped('b')])
    const state = { newest: '2026-11-10', missing: [UPPER] }
    storedState({ 'id-a': state, 'id-b': state })
    h.fetch.fetchBulkDailyRange.mockResolvedValue({
      kind: FetchErrorKind.TRANSIENT,
      statusCode: 503,
      message: 'HTTP 503',
    })

    const { error } = await settle(activities.backfillDailyLane(CUTOFF, 128, 0, 1, UPPER), MIN)

    expect(String(error)).toMatch(/Failed to fetch bulk daily downloads/)
    expect(h.dal.markDailyDownloadsProcessed).not.toHaveBeenCalled()
  })

  it('marks the batch processed with an error on a bulk 4xx', async () => {
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([unscoped('a'), unscoped('b')])
    const state = { newest: '2026-11-10', missing: [UPPER] }
    storedState({ 'id-a': state, 'id-b': state })
    h.fetch.fetchBulkDailyRange.mockResolvedValue({
      kind: FetchErrorKind.TRANSIENT,
      statusCode: 400,
      message: 'HTTP 400',
    })

    const { error } = await settle(activities.backfillDailyLane(CUTOFF, 128, 0, 1, UPPER), MIN)

    expect(error).toBeUndefined()
    for (const purl of ['pkg:npm/a', 'pkg:npm/b']) {
      expect(markedWith(purl)).toEqual([
        expect.objectContaining({ status: 'error', httpStatus: 400 }),
      ])
    }
  })
})

describe('backfillDailyLane: scoped packages weekly', () => {
  it('asks for scoped packages last processed at least 6 days before the cutoff', async () => {
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([])

    const { value, error } = await settle(
      activities.backfillDailyLane(CUTOFF, 128, 0, 1, UPPER),
      MIN,
    )

    expect(error).toBeUndefined()
    expect(value).toEqual({ fetched: 0 })
    expect(h.dal.getNpmPackagesNeedingDailyBackfill).toHaveBeenCalledWith(
      expect.anything(),
      CUTOFF,
      128,
      0,
      1,
      6,
    )
  })
})

describe('backfillDailyLane: repair from a date', () => {
  const REFETCH_FROM = '2026-05-01'

  it('refetches every day from refetchFrom to the bound with the weekly rule off', async () => {
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([scoped('a')])
    storedState({ 'id-a': { newest: UPPER, missing: [] } })

    const { error } = await settle(
      activities.backfillDailyLane(CUTOFF, 128, 0, 1, UPPER, REFETCH_FROM),
      MIN,
    )

    expect(error).toBeUndefined()
    expect(h.dal.getNpmPackagesNeedingDailyBackfill).toHaveBeenCalledWith(
      expect.anything(),
      CUTOFF,
      128,
      0,
      1,
      0,
    )
    expect(h.fetch.fetchDailyRange.mock.calls.map((c) => c.slice(0, 3))).toEqual([
      ['@scope/a', REFETCH_FROM, UPPER],
    ])
    expect(upsertedDays('id-a')).toEqual(days(REFETCH_FROM, UPPER))
    expect(markedWith('pkg:npm/%40scope/a')).toEqual([{ status: 'success' }])
  })

  it('starts at the first release when it is later than refetchFrom', async () => {
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([scoped('a', '2026-08-01')])
    storedState({ 'id-a': { newest: UPPER, missing: [] } })

    const { error } = await settle(
      activities.backfillDailyLane(CUTOFF, 128, 0, 1, UPPER, REFETCH_FROM),
      MIN,
    )

    expect(error).toBeUndefined()
    expect(h.fetch.fetchDailyRange.mock.calls[0].slice(0, 3)).toEqual([
      '@scope/a',
      '2026-08-01',
      UPPER,
    ])
  })

  it('repairs unscoped packages with one bulk request', async () => {
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([unscoped('a'), unscoped('b')])
    const state = { newest: UPPER, missing: [] }
    storedState({ 'id-a': state, 'id-b': state })

    const { error } = await settle(
      activities.backfillDailyLane(CUTOFF, 128, 0, 1, UPPER, REFETCH_FROM),
      MIN,
    )

    expect(error).toBeUndefined()
    expect(h.fetch.fetchBulkDailyRange.mock.calls.map((c) => c.slice(0, 3))).toEqual([
      [['a', 'b'], REFETCH_FROM, UPPER],
    ])
    expect(h.fetch.fetchDailyRange).not.toHaveBeenCalled()
  })
})

describe('backfillDailyLane: bulk path budget and heartbeat', () => {
  it('heartbeats while a bulk request waits out a 429', async () => {
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([unscoped('a'), unscoped('b')])
    const state = { newest: '2026-11-10', missing: [UPPER] }
    storedState({ 'id-a': state, 'id-b': state })
    h.fetch.fetchBulkDailyRange.mockResolvedValueOnce(rateLimited)

    const { value, error } = await settle(
      activities.backfillDailyLane(CUTOFF, 128, 0, 1, UPPER),
      2 * MIN,
    )

    expect(error).toBeUndefined()
    expect(value).toEqual({ fetched: 2 })
    expect(h.fetch.fetchBulkDailyRange).toHaveBeenCalledTimes(2)
    expect(h.ctx.heartbeat.mock.calls.length).toBeGreaterThanOrEqual(2)
    expect(h.ctx.heartbeat).toHaveBeenCalledWith({ laneIndex: 0, phase: 'daily' })
  })

  it('returns its progress as a normal result once the lane budget passes between bulk requests', async () => {
    const names = Array.from({ length: 130 }, (_, i) => `p${i}`)
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue(names.map((n) => unscoped(n)))
    storedState(
      Object.fromEntries(names.map((n) => [`id-${n}`, { newest: '2026-11-10', missing: [UPPER] }])),
    )
    h.fetch.fetchBulkDailyRange.mockImplementation(async (batch: string[], start: string) => {
      await new Promise((resolve) => setTimeout(resolve, 21 * MIN))
      return { downloads: new Map(batch.map((n) => [n, rows(start, UPPER)])) }
    })

    const { value, error } = await settle(
      activities.backfillDailyLane(CUTOFF, 130, 0, 1, UPPER),
      30 * MIN,
    )

    expect(error).toBeUndefined()
    expect(value).toEqual({ fetched: 130 })
    expect(h.fetch.fetchBulkDailyRange).toHaveBeenCalledTimes(1)
    expect(h.dal.markDailyDownloadsProcessed).toHaveBeenCalledTimes(128)
    expect(markedWith('pkg:npm/p129')).toEqual([])
  })

  it('logs the rows it wrote when it stops on a long block', async () => {
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([scoped('a'), scoped('b')])
    const state = { newest: '2026-11-10', missing: [UPPER] }
    storedState({ 'id-a': state, 'id-b': state })
    h.dal.upsertDailyDownloads.mockResolvedValue({ inserted: 1, overwritten: 1 })
    h.fetch.fetchDailyRange.mockImplementation(async (name: string, start: string, end: string) =>
      name === '@scope/a' ? { start, end, downloads: rows(start, end) } : rateLimited,
    )

    const { value, error } = await settle(
      activities.backfillDailyLane(CUTOFF, 128, 0, 1, UPPER),
      30 * MIN,
    )

    expect(error).toBeUndefined()
    expect(value).toEqual({ fetched: 2, blocked: true })
    expect(h.log.info).toHaveBeenCalledWith(
      expect.objectContaining({ inserted: 1, overwritten: 1 }),
      'Backfilled daily downloads lane',
    )
  })
})
