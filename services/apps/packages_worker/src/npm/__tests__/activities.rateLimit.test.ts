import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import * as activities from '../activities'
import { DownloadsThrottle } from '../downloadsThrottle'
import { type FetchError, FetchErrorKind } from '../types'

const h = vi.hoisted(() => ({
  throttle: { current: undefined as unknown },
  ctx: {
    heartbeat: vi.fn(),
    info: { attempt: 1, scheduledTimestampMs: 0 },
  },
  dal: {
    getExistingLast30dEndDates: vi.fn(),
    getMissingDownloadDates: vi.fn(),
    getNpmChangesLastSeq: vi.fn(),
    getNpmPackagesNeedingDailyBackfill: vi.fn(),
    getNpmPurlsDueForLast30dHistory: vi.fn(),
    getNpmPurlsDueForLatest30d: vi.fn(),
    getNpmPurlsForChangedNames: vi.fn(),
    getNpmPurlsScannedSince: vi.fn(),
    getUnscannedNpmPurls: vi.fn(),
    insertDailyDownloads: vi.fn(),
    logAuditFieldChanges: vi.fn(),
    markDailyDownloadsProcessed: vi.fn(),
    markLast30dHistoryBackfilled: vi.fn(),
    markLast30dProcessed: vi.fn(),
    markNpmPackageScanned: vi.fn(),
    setNpmChangesLastSeq: vi.fn(),
    upsertLast30dDownload: vi.fn(),
  },
  fetch: {
    fetchBulkPointRange: vi.fn(),
    fetchDailyRange: vi.fn(),
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
  getServiceChildLogger: () => ({ info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() }),
}))
vi.mock('../../db', () => ({ getPackagesDb: vi.fn(async () => ({})) }))
vi.mock('../fetchDownloads', () => h.fetch)
// The limiter is process-wide state, so each test gets a fresh one through the accessor.
vi.mock('../downloadsThrottle', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../downloadsThrottle')>()),
  npmDownloadsThrottle: () => h.throttle.current,
}))

const MIN = 60_000
const CUTOFF = '2026-11-12T00:00:00.000Z'

const rateLimited: FetchError = {
  kind: FetchErrorKind.RATE_LIMIT,
  statusCode: 429,
  message: 'rate limited by npm downloads API',
}

const dailyOk = {
  start: '2026-11-10',
  end: '2026-11-11',
  downloads: [
    { day: '2026-11-10', downloads: 10 },
    { day: '2026-11-11', downloads: 12 },
  ],
}

function candidate(name: string) {
  return { id: `id-${name}`, purl: `pkg:npm/${name}`, name, firstReleaseAt: '2026-11-01' }
}

async function loadActivities() {
  return activities
}

// Lets a pending activity run against the fake clock and captures its outcome without an
// unhandled rejection.
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
  h.throttle.current = new DownloadsThrottle(1000, { info: vi.fn(), warn: vi.fn() })
  vi.setSystemTime(new Date('2026-11-12T12:00:00Z'))
  vi.stubEnv('CROWD_PACKAGES_DOWNLOAD_SLEEP_MS', '1000')
  vi.stubEnv('CROWD_PACKAGES_PROXIES_ENABLED', 'false')
  for (const fn of [...Object.values(h.dal), ...Object.values(h.fetch), h.ctx.heartbeat]) {
    fn.mockReset()
  }
  h.dal.getMissingDownloadDates.mockResolvedValue(['2026-11-10', '2026-11-11'])
  h.dal.insertDailyDownloads.mockResolvedValue([])
  h.dal.upsertLast30dDownload.mockResolvedValue([])
  h.dal.getExistingLast30dEndDates.mockResolvedValue([])
})

afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllEnvs()
})

describe('backfillDailyLane', () => {
  it('pauses on a 429, retries the same package and completes (regression)', async () => {
    const { backfillDailyLane } = await loadActivities()
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([candidate('foo')])
    h.fetch.fetchDailyRange.mockResolvedValueOnce(rateLimited).mockResolvedValueOnce(dailyOk)

    const { value, error } = await settle(backfillDailyLane(CUTOFF, 20, 0, 1), 2 * MIN)

    expect(error).toBeUndefined()
    expect(value).toMatchObject({ fetched: 1 })
    expect(value?.blocked).toBeFalsy()
    expect(h.fetch.fetchDailyRange).toHaveBeenCalledTimes(2)
    expect(h.fetch.fetchDailyRange.mock.calls[1].slice(0, 3)).toEqual([
      'foo',
      '2026-11-10',
      '2026-11-11',
    ])
    expect(h.dal.insertDailyDownloads).toHaveBeenCalledTimes(1)
    expect(h.dal.markDailyDownloadsProcessed).toHaveBeenCalledWith(
      expect.anything(),
      'pkg:npm/foo',
      { status: 'success' },
    )
  })

  it('heartbeats before each fetch', async () => {
    const { backfillDailyLane } = await loadActivities()
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([candidate('foo'), candidate('bar')])
    h.fetch.fetchDailyRange.mockResolvedValue(dailyOk)

    const { error } = await settle(backfillDailyLane(CUTOFF, 20, 0, 1), MIN)

    expect(error).toBeUndefined()
    expect(h.fetch.fetchDailyRange).toHaveBeenCalledTimes(2)
    expect(h.ctx.heartbeat.mock.calls.length).toBeGreaterThanOrEqual(2)
  })

  it('returns blocked on a long block, keeping finished packages and leaving the rest due', async () => {
    const { backfillDailyLane } = await loadActivities()
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([candidate('foo'), candidate('bar')])
    h.fetch.fetchDailyRange.mockImplementation(async (name: string) =>
      name === 'foo' ? dailyOk : rateLimited,
    )

    const { value, error } = await settle(backfillDailyLane(CUTOFF, 20, 0, 1), 30 * MIN)

    expect(error).toBeUndefined()
    expect(value).toEqual({ fetched: 2, blocked: true })
    expect(h.dal.markDailyDownloadsProcessed).toHaveBeenCalledTimes(1)
    expect(h.dal.markDailyDownloadsProcessed).toHaveBeenCalledWith(
      expect.anything(),
      'pkg:npm/foo',
      { status: 'success' },
    )
  })

  it('still skips a 404 package after one fetch', async () => {
    const { backfillDailyLane } = await loadActivities()
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([candidate('gone')])
    h.fetch.fetchDailyRange.mockResolvedValue({
      kind: FetchErrorKind.NOT_FOUND,
      statusCode: 404,
      message: 'gone not found',
    })

    const { value, error } = await settle(backfillDailyLane(CUTOFF, 20, 0, 1), MIN)

    expect(error).toBeUndefined()
    expect(value).toMatchObject({ fetched: 1 })
    expect(h.fetch.fetchDailyRange).toHaveBeenCalledTimes(1)
    expect(h.dal.markDailyDownloadsProcessed).toHaveBeenCalledWith(
      expect.anything(),
      'pkg:npm/gone',
      expect.objectContaining({ status: 'error', httpStatus: 404 }),
    )
  })

  it('still throws on a 5xx so Temporal retries the lane', async () => {
    const { backfillDailyLane } = await loadActivities()
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([candidate('foo')])
    h.fetch.fetchDailyRange.mockResolvedValue({
      kind: FetchErrorKind.TRANSIENT,
      statusCode: 503,
      message: 'HTTP 503',
    })

    const { error } = await settle(backfillDailyLane(CUTOFF, 20, 0, 1), MIN)

    expect(String(error)).toMatch(/Failed to fetch daily downloads for foo/)
  })
})

describe('refreshLatestLast30dLane', () => {
  it('pauses on a 429, retries and stores the latest window', async () => {
    const { refreshLatestLast30dLane } = await loadActivities()
    h.dal.getNpmPurlsDueForLatest30d.mockResolvedValue([
      { purl: 'pkg:npm/%40scope/pkg', firstReleaseAt: null },
    ])
    h.fetch.fetchPointRange
      .mockResolvedValueOnce(rateLimited)
      .mockResolvedValueOnce({ count: 7, start: '2026-10-02', end: '2026-11-01' })

    const { value, error } = await settle(refreshLatestLast30dLane(CUTOFF, 128, 0, 1), 2 * MIN)

    expect(error).toBeUndefined()
    expect(value).toMatchObject({ fetched: 1 })
    expect(value?.blocked).toBeFalsy()
    expect(h.fetch.fetchPointRange).toHaveBeenCalledTimes(2)
    expect(h.dal.upsertLast30dDownload).toHaveBeenCalledWith(
      expect.anything(),
      'pkg:npm/%40scope/pkg',
      '2026-10-02',
      '2026-11-01',
      7,
      true,
    )
    expect(h.dal.markLast30dProcessed).toHaveBeenCalledWith(
      expect.anything(),
      'pkg:npm/%40scope/pkg',
      { status: 'success' },
    )
  })

  it('returns blocked on a long block without marking the package processed', async () => {
    const { refreshLatestLast30dLane } = await loadActivities()
    h.dal.getNpmPurlsDueForLatest30d.mockResolvedValue([
      { purl: 'pkg:npm/foo', firstReleaseAt: null },
    ])
    h.fetch.fetchBulkPointRange.mockResolvedValue(rateLimited)

    const { value, error } = await settle(refreshLatestLast30dLane(CUTOFF, 128, 0, 1), 30 * MIN)

    expect(error).toBeUndefined()
    expect(value).toEqual({ fetched: 1, blocked: true })
    expect(h.dal.markLast30dProcessed).not.toHaveBeenCalled()
  })
})

describe('backfillLast30dHistoryLane', () => {
  it('returns blocked on a long block without marking history backfilled', async () => {
    const { backfillLast30dHistoryLane } = await loadActivities()
    h.dal.getNpmPurlsDueForLast30dHistory.mockResolvedValue([
      { purl: 'pkg:npm/foo', firstReleaseAt: '2026-08-15' },
    ])
    h.dal.getExistingLast30dEndDates.mockResolvedValue(['2026-11-01'])
    h.fetch.fetchBulkPointRange.mockResolvedValue(rateLimited)

    const { value, error } = await settle(backfillLast30dHistoryLane(50, 6, 0, 1), 30 * MIN)

    expect(error).toBeUndefined()
    expect(value).toEqual({ fetched: 1, blocked: true })
    expect(h.dal.markLast30dHistoryBackfilled).not.toHaveBeenCalled()
  })
})

describe('shared limiter', () => {
  it('spaces requests from the daily and 30-day flows on the same egress', async () => {
    const { backfillDailyLane, refreshLatestLast30dLane } = await loadActivities()
    const starts: number[] = []
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([candidate('foo')])
    h.dal.getNpmPurlsDueForLatest30d.mockResolvedValue([
      { purl: 'pkg:npm/%40scope/pkg', firstReleaseAt: null },
    ])
    h.fetch.fetchDailyRange.mockImplementation(async () => {
      starts.push(Date.now())
      return dailyOk
    })
    h.fetch.fetchPointRange.mockImplementation(async () => {
      starts.push(Date.now())
      return { count: 7, start: '2026-10-02', end: '2026-11-01' }
    })

    const daily = settle(backfillDailyLane(CUTOFF, 20, 0, 1), 0)
    const latest = settle(refreshLatestLast30dLane(CUTOFF, 128, 0, 1), 0)
    await vi.advanceTimersByTimeAsync(10_000)
    const [d, l] = await Promise.all([daily, latest])

    expect(d.error).toBeUndefined()
    expect(l.error).toBeUndefined()
    expect(starts).toHaveLength(2)
    expect(Math.abs(starts[1] - starts[0])).toBeGreaterThanOrEqual(1000)
  })
})
