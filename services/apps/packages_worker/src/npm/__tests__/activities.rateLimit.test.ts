import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import * as activities from '../activities'
import { createDownloadsThrottle, type DownloadsThrottle } from '../downloadsThrottle'
import { type FetchError, FetchErrorKind } from '../types'

const h = vi.hoisted(() => ({
  throttle: { current: undefined as unknown },
  ctx: {
    heartbeat: vi.fn(),
    info: { attempt: 1, scheduledTimestampMs: 0 },
    cancellationSignal: undefined as AbortSignal | undefined,
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
    getNewestDownloadDate: vi.fn(),
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
const UPPER = '2026-11-11'

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

// Scoped packages take one request each, while two or more unscoped packages share a bulk request.
function scopedCandidate(name: string) {
  return {
    id: `id-${name}`,
    purl: `pkg:npm/%40scope/${name}`,
    name: `@scope/${name}`,
    firstReleaseAt: '2026-11-01',
  }
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
  h.throttle.current = createDownloadsThrottle(1000, { info: vi.fn(), warn: vi.fn() })
  h.ctx.cancellationSignal = undefined
  vi.setSystemTime(new Date('2026-11-12T12:00:00Z'))
  vi.stubEnv('CROWD_PACKAGES_DOWNLOAD_SLEEP_MS', '1000')
  vi.stubEnv('CROWD_PACKAGES_PROXIES_ENABLED', 'false')
  for (const fn of [...Object.values(h.dal), ...Object.values(h.fetch), h.ctx.heartbeat]) {
    fn.mockReset()
  }
  h.dal.getMissingDownloadDates.mockResolvedValue(['2026-11-10', '2026-11-11'])
  h.dal.upsertDailyDownloads.mockResolvedValue({ inserted: 0, overwritten: 0 })
  h.dal.getNewestDownloadDate.mockResolvedValue(null)
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

    const { value, error } = await settle(backfillDailyLane(CUTOFF, 20, 0, 1, UPPER), 2 * MIN)

    expect(error).toBeUndefined()
    expect(value).toMatchObject({ fetched: 1 })
    expect(value?.blocked).toBeFalsy()
    expect(h.fetch.fetchDailyRange).toHaveBeenCalledTimes(2)
    expect(h.fetch.fetchDailyRange.mock.calls[1].slice(0, 3)).toEqual([
      'foo',
      '2026-11-10',
      '2026-11-11',
    ])
    expect(h.dal.upsertDailyDownloads).toHaveBeenCalledTimes(1)
    expect(h.dal.markDailyDownloadsProcessed).toHaveBeenCalledWith(
      expect.anything(),
      'pkg:npm/foo',
      { status: 'success' },
    )
  })

  it('heartbeats before each fetch', async () => {
    const { backfillDailyLane } = await loadActivities()
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([
      scopedCandidate('foo'),
      scopedCandidate('bar'),
    ])
    h.fetch.fetchDailyRange.mockResolvedValue(dailyOk)

    const { error } = await settle(backfillDailyLane(CUTOFF, 20, 0, 1, UPPER), MIN)

    expect(error).toBeUndefined()
    expect(h.fetch.fetchDailyRange).toHaveBeenCalledTimes(2)
    expect(h.ctx.heartbeat.mock.calls.length).toBeGreaterThanOrEqual(2)
  })

  it('returns blocked on a long block, keeping finished packages and leaving the rest due', async () => {
    const { backfillDailyLane } = await loadActivities()
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([
      scopedCandidate('foo'),
      scopedCandidate('bar'),
    ])
    h.fetch.fetchDailyRange.mockImplementation(async (name: string) =>
      name === '@scope/foo' ? dailyOk : rateLimited,
    )

    const { value, error } = await settle(backfillDailyLane(CUTOFF, 20, 0, 1, UPPER), 30 * MIN)

    expect(error).toBeUndefined()
    expect(value).toEqual({ fetched: 2, blocked: true })
    expect(h.dal.markDailyDownloadsProcessed).toHaveBeenCalledTimes(1)
    expect(h.dal.markDailyDownloadsProcessed).toHaveBeenCalledWith(
      expect.anything(),
      'pkg:npm/%40scope/foo',
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

    const { value, error } = await settle(backfillDailyLane(CUTOFF, 20, 0, 1, UPPER), MIN)

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

    const { error } = await settle(backfillDailyLane(CUTOFF, 20, 0, 1, UPPER), MIN)

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

    const daily = settle(backfillDailyLane(CUTOFF, 20, 0, 1, UPPER), 0)
    const latest = settle(refreshLatestLast30dLane(CUTOFF, 128, 0, 1), 0)
    await vi.advanceTimersByTimeAsync(10_000)
    const [d, l] = await Promise.all([daily, latest])

    expect(d.error).toBeUndefined()
    expect(l.error).toBeUndefined()
    expect(starts).toHaveLength(2)
    expect(Math.abs(starts[1] - starts[0])).toBeGreaterThanOrEqual(1000)
  })
})

describe('partial progress during a block', () => {
  it('daily: the in-progress package keeps rows from earlier chunks and stays unmarked', async () => {
    const { backfillDailyLane } = await loadActivities()
    // 600 consecutive days split into a 540 day chunk and a 60 day chunk.
    const dates = Array.from({ length: 600 }, (_, i) =>
      new Date(Date.UTC(2024, 0, 1 + i)).toISOString().slice(0, 10),
    )
    h.dal.getMissingDownloadDates.mockResolvedValue(dates)
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([candidate('bar')])
    h.fetch.fetchDailyRange.mockResolvedValueOnce(dailyOk).mockResolvedValue(rateLimited)

    const { value, error } = await settle(backfillDailyLane(CUTOFF, 20, 0, 1, UPPER), 30 * MIN)

    expect(error).toBeUndefined()
    expect(value).toEqual({ fetched: 1, blocked: true })
    expect(h.dal.upsertDailyDownloads).toHaveBeenCalledTimes(1)
    expect(h.dal.markDailyDownloadsProcessed).not.toHaveBeenCalled()
  })

  it('latest: a purl settled before the block stays marked, the blocked one does not', async () => {
    const { refreshLatestLast30dLane } = await loadActivities()
    h.dal.getNpmPurlsDueForLatest30d.mockResolvedValue([
      { purl: 'pkg:npm/foo', firstReleaseAt: null },
      { purl: 'pkg:npm/%40scope/pkg', firstReleaseAt: null },
    ])
    h.fetch.fetchBulkPointRange.mockResolvedValue({
      counts: new Map([['foo', 9]]),
      start: '2026-10-02',
      end: '2026-11-01',
    })
    h.fetch.fetchPointRange.mockResolvedValue(rateLimited)

    const { value, error } = await settle(refreshLatestLast30dLane(CUTOFF, 128, 0, 1), 30 * MIN)

    expect(error).toBeUndefined()
    expect(value).toEqual({ fetched: 2, blocked: true })
    expect(h.dal.upsertLast30dDownload).toHaveBeenCalledWith(
      expect.anything(),
      'pkg:npm/foo',
      '2026-10-02',
      '2026-11-01',
      9,
      true,
    )
    expect(h.dal.markLast30dProcessed).toHaveBeenCalledTimes(1)
    expect(h.dal.markLast30dProcessed).toHaveBeenCalledWith(expect.anything(), 'pkg:npm/foo', {
      status: 'success',
    })
  })
})

describe('lane time budget', () => {
  it('daily lane returns its progress as a normal result once 20 minutes have passed', async () => {
    const { backfillDailyLane } = await loadActivities()
    const names = Array.from({ length: 30 }, (_, i) => `pkg${i}`)
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue(names.map((n) => scopedCandidate(n)))
    h.fetch.fetchDailyRange.mockImplementation(async () => {
      await new Promise((resolve) => setTimeout(resolve, 2 * MIN))
      return dailyOk
    })

    const { value, error } = await settle(backfillDailyLane(CUTOFF, 30, 0, 1, UPPER), 40 * MIN)

    expect(error).toBeUndefined()
    expect(value).toEqual({ fetched: 30 })
    const fetched = h.fetch.fetchDailyRange.mock.calls.length
    expect(fetched).toBeGreaterThan(0)
    expect(fetched).toBeLessThan(30)
    expect(h.dal.markDailyDownloadsProcessed).toHaveBeenCalledTimes(fetched)
    expect(h.dal.markDailyDownloadsProcessed).not.toHaveBeenCalledWith(
      expect.anything(),
      'pkg:npm/%40scope/pkg29',
      expect.anything(),
    )
  })

  it('latest lane returns its progress as a normal result once 20 minutes have passed', async () => {
    const { refreshLatestLast30dLane } = await loadActivities()
    h.dal.getNpmPurlsDueForLatest30d.mockResolvedValue(
      Array.from({ length: 5 }, (_, i) => ({
        purl: `pkg:npm/%40scope/p${i}`,
        firstReleaseAt: null,
      })),
    )
    h.fetch.fetchPointRange.mockImplementation(async () => {
      await new Promise((resolve) => setTimeout(resolve, 6 * MIN))
      return { count: 7, start: '2026-10-02', end: '2026-11-01' }
    })

    const { value, error } = await settle(refreshLatestLast30dLane(CUTOFF, 128, 0, 1), 60 * MIN)

    expect(error).toBeUndefined()
    expect(value).toEqual({ fetched: 5 })
    const fetched = h.fetch.fetchPointRange.mock.calls.length
    expect(fetched).toBeGreaterThan(0)
    expect(fetched).toBeLessThan(5)
    expect(h.dal.markLast30dProcessed).toHaveBeenCalledTimes(fetched)
  })
})

describe('cancellation', () => {
  it('a cancelled lane stops before fetching', async () => {
    const { backfillDailyLane } = await loadActivities()
    h.ctx.cancellationSignal = AbortSignal.abort()
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([candidate('foo')])
    h.fetch.fetchDailyRange.mockResolvedValue(dailyOk)

    const { error } = await settle(backfillDailyLane(CUTOFF, 20, 0, 1, UPPER), MIN)

    expect(error).toBeDefined()
    expect(h.fetch.fetchDailyRange).not.toHaveBeenCalled()
    expect(h.dal.markDailyDownloadsProcessed).not.toHaveBeenCalled()
  })
})

describe('lane deadline during a long block', () => {
  // 429s land at about 0, 1, 3, 7 and 15 minutes. Past its 20 minute budget the lane yields on the
  // next wait wake-up, before the probe it would otherwise send at 25 minutes.
  it('daily lane yields at its budget instead of probing again', async () => {
    const { backfillDailyLane } = await loadActivities()
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([candidate('foo')])
    h.fetch.fetchDailyRange.mockResolvedValue(rateLimited)

    const { value, error } = await settle(backfillDailyLane(CUTOFF, 20, 0, 1, UPPER), 30 * MIN)

    expect(error).toBeUndefined()
    expect(value).toEqual({ fetched: 1, blocked: true })
    expect(h.fetch.fetchDailyRange).toHaveBeenCalledTimes(5)
  })

  it('latest lane yields at its budget instead of probing again', async () => {
    const { refreshLatestLast30dLane } = await loadActivities()
    h.dal.getNpmPurlsDueForLatest30d.mockResolvedValue([
      { purl: 'pkg:npm/foo', firstReleaseAt: null },
    ])
    h.fetch.fetchBulkPointRange.mockResolvedValue(rateLimited)

    const { value, error } = await settle(refreshLatestLast30dLane(CUTOFF, 128, 0, 1), 30 * MIN)

    expect(error).toBeUndefined()
    expect(value).toEqual({ fetched: 1, blocked: true })
    expect(h.fetch.fetchBulkPointRange).toHaveBeenCalledTimes(5)
  })

  it('history lane yields at its budget instead of probing again', async () => {
    const { backfillLast30dHistoryLane } = await loadActivities()
    h.dal.getNpmPurlsDueForLast30dHistory.mockResolvedValue([
      { purl: 'pkg:npm/foo', firstReleaseAt: '2026-08-15' },
    ])
    h.dal.getExistingLast30dEndDates.mockResolvedValue(['2026-11-01'])
    h.fetch.fetchBulkPointRange.mockResolvedValue(rateLimited)

    const { value, error } = await settle(backfillLast30dHistoryLane(50, 6, 0, 1), 30 * MIN)

    expect(error).toBeUndefined()
    expect(value).toEqual({ fetched: 1, blocked: true })
    expect(h.fetch.fetchBulkPointRange).toHaveBeenCalledTimes(5)
  })
})

describe('lane deadline while another flow succeeds', () => {
  // Another flow's success every 30s keeps resetting the shared block clock, so the egress never
  // counts as blocked while this lane's own probes keep getting 429s.
  function succeedElsewhere() {
    const throttle = h.throttle.current as DownloadsThrottle
    return setInterval(() => throttle.recordSuccess('direct'), 30_000)
  }

  // Unlike settle, this never awaits a lane that has not finished by the end of the advance.
  async function settleWithin<T>(promise: Promise<T>, advanceMs: number) {
    const outcome: { done: boolean; value?: T; error?: unknown } = { done: false }
    promise.then(
      (value) => Object.assign(outcome, { done: true, value }),
      (error: unknown) => Object.assign(outcome, { done: true, error }),
    )
    await vi.advanceTimersByTimeAsync(advanceMs)
    return outcome
  }

  it('daily lane returns its progress without blocked within 42 minutes', async () => {
    const { backfillDailyLane } = await loadActivities()
    h.dal.getNpmPackagesNeedingDailyBackfill.mockResolvedValue([candidate('foo')])
    h.fetch.fetchDailyRange.mockResolvedValue(rateLimited)
    const timer = succeedElsewhere()

    const outcome = await settleWithin(backfillDailyLane(CUTOFF, 20, 0, 1, UPPER), 42 * MIN)
    clearInterval(timer)

    expect(outcome).toEqual({ done: true, value: { fetched: 1 } })
    expect(h.dal.upsertDailyDownloads).not.toHaveBeenCalled()
    expect(h.dal.markDailyDownloadsProcessed).not.toHaveBeenCalled()
  })

  it('latest lane returns its progress without blocked on a scoped package', async () => {
    const { refreshLatestLast30dLane } = await loadActivities()
    h.dal.getNpmPurlsDueForLatest30d.mockResolvedValue([
      { purl: 'pkg:npm/%40scope/pkg', firstReleaseAt: null },
    ])
    h.fetch.fetchPointRange.mockResolvedValue(rateLimited)
    const timer = succeedElsewhere()

    const outcome = await settleWithin(refreshLatestLast30dLane(CUTOFF, 128, 0, 1), 42 * MIN)
    clearInterval(timer)

    expect(outcome).toEqual({ done: true, value: { fetched: 1 } })
    expect(h.dal.upsertLast30dDownload).not.toHaveBeenCalled()
    expect(h.dal.markLast30dProcessed).not.toHaveBeenCalled()
  })

  it('history lane returns its progress without blocked on a bulk batch', async () => {
    const { backfillLast30dHistoryLane } = await loadActivities()
    h.dal.getNpmPurlsDueForLast30dHistory.mockResolvedValue([
      { purl: 'pkg:npm/foo', firstReleaseAt: '2026-08-15' },
    ])
    h.dal.getExistingLast30dEndDates.mockResolvedValue(['2026-11-01'])
    h.fetch.fetchBulkPointRange.mockResolvedValue(rateLimited)
    const timer = succeedElsewhere()

    const outcome = await settleWithin(backfillLast30dHistoryLane(50, 6, 0, 1), 42 * MIN)
    clearInterval(timer)

    expect(outcome).toEqual({ done: true, value: { fetched: 1 } })
    expect(h.dal.upsertLast30dDownload).not.toHaveBeenCalled()
    expect(h.dal.markLast30dHistoryBackfilled).not.toHaveBeenCalled()
  })
})
