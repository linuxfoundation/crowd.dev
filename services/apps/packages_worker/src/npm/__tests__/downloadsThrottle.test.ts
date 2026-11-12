import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { createDownloadsThrottle, downloadsFloorMs, isDownloadsBlocked } from '../downloadsThrottle'
import { type FetchError, FetchErrorKind } from '../types'

const MIN = 60_000
const FLOOR = 1000

const rateLimited: FetchError = {
  kind: FetchErrorKind.RATE_LIMIT,
  statusCode: 429,
  message: 'rate limited',
}

function makeLog() {
  return { info: vi.fn(), warn: vi.fn() }
}

// Records the fake-clock time at which each promise settles, relative to the test start.
function settleTimes(start: number) {
  const times: number[] = []
  const track = (p: Promise<unknown>) => p.then(() => times.push(Date.now() - start))
  return { times, track }
}

beforeEach(() => {
  vi.useFakeTimers()
  vi.setSystemTime(new Date('2026-11-12T12:00:00Z'))
})

afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllEnvs()
})

describe('downloadsFloorMs', () => {
  it.each([
    [undefined, 1000],
    ['4500', 4500],
    ['abc', 1000],
    ['0', 1000],
    ['-5', 1000],
  ])('CROWD_PACKAGES_DOWNLOAD_SLEEP_MS=%s gives a %ims floor', (raw, expected) => {
    vi.stubEnv('CROWD_PACKAGES_DOWNLOAD_SLEEP_MS', raw)
    expect(downloadsFloorMs()).toBe(expected)
  })
})

describe('acquire', () => {
  it('starts at the floor interval', () => {
    expect(createDownloadsThrottle(FLOOR, makeLog()).intervalMs('direct')).toBe(FLOOR)
  })

  it('spaces request starts on the same egress by the interval', async () => {
    const throttle = createDownloadsThrottle(FLOOR, makeLog())
    const { times, track } = settleTimes(Date.now())

    track(throttle.acquire('direct'))
    await vi.advanceTimersByTimeAsync(0)
    track(throttle.acquire('direct'))
    await vi.advanceTimersByTimeAsync(FLOOR - 1)
    expect(times).toEqual([0])

    await vi.advanceTimersByTimeAsync(1)
    expect(times).toEqual([0, FLOOR])
  })

  it('serializes concurrent callers on the same egress', async () => {
    const throttle = createDownloadsThrottle(FLOOR, makeLog())
    const { times, track } = settleTimes(Date.now())

    track(throttle.acquire('direct'))
    track(throttle.acquire('direct'))
    track(throttle.acquire('direct'))
    await vi.advanceTimersByTimeAsync(5 * FLOOR)

    expect(times).toEqual([0, FLOOR, 2 * FLOOR])
  })

  it('keeps different egress keys independent', async () => {
    const throttle = createDownloadsThrottle(FLOOR, makeLog())
    const { times, track } = settleTimes(Date.now())

    track(throttle.acquire('direct'))
    track(throttle.acquire('proxy-a.example'))
    await vi.advanceTimersByTimeAsync(0)

    expect(times).toEqual([0, 0])
  })
})

describe('back off on 429', () => {
  it('doubles the interval and pauses the egress for a minute', async () => {
    const throttle = createDownloadsThrottle(FLOOR, makeLog())
    const { times, track } = settleTimes(Date.now())

    throttle.recordRateLimit('direct')
    expect(throttle.intervalMs('direct')).toBe(2 * FLOOR)

    track(throttle.acquire('direct'))
    await vi.advanceTimersByTimeAsync(MIN - 1)
    expect(times).toEqual([])
    await vi.advanceTimersByTimeAsync(1)
    expect(times).toEqual([MIN])
  })

  it('doubles the pause per consecutive 429 up to 10 minutes and caps the interval at 60s', async () => {
    const log = makeLog()
    const throttle = createDownloadsThrottle(FLOOR, log)

    for (let i = 0; i < 7; i++) throttle.recordRateLimit('direct')

    const payloads = log.warn.mock.calls
      .filter(([, msg]) => msg === 'npm downloads rate limited')
      .map(([obj]) => obj)
    expect(payloads.map((p) => p.pauseMs)).toEqual([
      MIN,
      2 * MIN,
      4 * MIN,
      8 * MIN,
      10 * MIN,
      10 * MIN,
      10 * MIN,
    ])
    expect(payloads.map((p) => p.intervalMs)).toEqual([
      2000, 4000, 8000, 16000, 32000, 60000, 60000,
    ])
    expect(payloads.map((p) => p.consecutive)).toEqual([1, 2, 3, 4, 5, 6, 7])
    expect(payloads[0]).toMatchObject({ egress: 'direct' })
    expect(throttle.intervalMs('direct')).toBe(60_000)

    const { times, track } = settleTimes(Date.now())
    track(throttle.acquire('direct'))
    await vi.advanceTimersByTimeAsync(10 * MIN - 1)
    expect(times).toEqual([])
    await vi.advanceTimersByTimeAsync(1)
    expect(times).toEqual([10 * MIN])
  })

  it('restarts the pause ladder at one minute after a success', async () => {
    const log = makeLog()
    const throttle = createDownloadsThrottle(FLOOR, log)

    throttle.recordRateLimit('direct')
    throttle.recordRateLimit('direct')
    await vi.advanceTimersByTimeAsync(3 * MIN)
    throttle.recordSuccess('direct')
    throttle.recordRateLimit('direct')

    const rateLimitLogs = log.warn.mock.calls.filter(
      ([, msg]) => msg === 'npm downloads rate limited',
    )
    expect(rateLimitLogs[rateLimitLogs.length - 1][0]).toMatchObject({
      pauseMs: MIN,
      consecutive: 1,
    })
  })
})

describe('recovery', () => {
  it('eases the interval 10% after 10 clean minutes, at most once per 10 minutes', async () => {
    const log = makeLog()
    const throttle = createDownloadsThrottle(FLOOR, log)

    throttle.recordRateLimit('direct')
    expect(throttle.intervalMs('direct')).toBe(2000)

    await vi.advanceTimersByTimeAsync(5 * MIN)
    throttle.recordSuccess('direct')
    expect(throttle.intervalMs('direct')).toBe(2000)

    await vi.advanceTimersByTimeAsync(5 * MIN)
    throttle.recordSuccess('direct')
    expect(throttle.intervalMs('direct')).toBe(1800)
    expect(log.info).toHaveBeenCalledWith(
      expect.objectContaining({ egress: 'direct', intervalMs: 1800 }),
      'npm downloads throttle eased',
    )

    throttle.recordSuccess('direct')
    expect(throttle.intervalMs('direct')).toBe(1800)

    await vi.advanceTimersByTimeAsync(10 * MIN)
    throttle.recordSuccess('direct')
    expect(throttle.intervalMs('direct')).toBe(1620)
  })

  it('never eases below the floor', async () => {
    const throttle = createDownloadsThrottle(FLOOR, makeLog())
    throttle.recordRateLimit('direct')

    for (let i = 0; i < 20; i++) {
      await vi.advanceTimersByTimeAsync(10 * MIN)
      throttle.recordSuccess('direct')
    }

    expect(throttle.intervalMs('direct')).toBe(FLOOR)
  })
})

describe('run', () => {
  it('waits out the pause after a 429, retries and returns the success', async () => {
    const throttle = createDownloadsThrottle(FLOOR, makeLog())
    const doFetch = vi
      .fn<() => Promise<{ count: number } | FetchError>>()
      .mockResolvedValueOnce(rateLimited)
      .mockResolvedValueOnce({ count: 5 })
    const onWait = vi.fn()

    const result = throttle.run('direct', doFetch, onWait)
    await vi.advanceTimersByTimeAsync(MIN + FLOOR)

    await expect(result).resolves.toEqual({ count: 5 })
    expect(doFetch).toHaveBeenCalledTimes(2)
    expect(onWait.mock.calls.length).toBeGreaterThanOrEqual(2)
  })

  it('calls onWait at least every 30 seconds while paused', async () => {
    const throttle = createDownloadsThrottle(FLOOR, makeLog())
    for (let i = 0; i < 5; i++) throttle.recordRateLimit('direct')
    const start = Date.now()
    const beats: number[] = []

    const result = throttle.run(
      'direct',
      async () => ({ count: 1 }),
      () => beats.push(Date.now() - start),
    )
    await vi.advanceTimersByTimeAsync(11 * MIN)
    await result

    const checkpoints = [0, ...beats, 10 * MIN]
    const gaps = checkpoints.slice(1).map((t, i) => t - checkpoints[i])
    expect(Math.max(...gaps)).toBeLessThanOrEqual(30_000)
  })

  it.each([
    { kind: FetchErrorKind.NOT_FOUND, statusCode: 404, message: 'not found' },
    { kind: FetchErrorKind.TRANSIENT, statusCode: 503, message: 'HTTP 503' },
    { kind: FetchErrorKind.MALFORMED, message: 'invalid JSON' },
  ] as FetchError[])('returns a $kind result unchanged without retrying', async (error) => {
    const throttle = createDownloadsThrottle(FLOOR, makeLog())
    const doFetch = vi.fn(async () => error)

    const result = throttle.run('direct', doFetch)
    await vi.advanceTimersByTimeAsync(MIN)

    await expect(result).resolves.toBe(error)
    expect(doFetch).toHaveBeenCalledTimes(1)
  })

  it('returns a blocked marker once only 429s have been seen for 20 minutes', async () => {
    const log = makeLog()
    const throttle = createDownloadsThrottle(FLOOR, log)
    const doFetch = vi.fn(async () => rateLimited)

    const result = throttle.run('direct', doFetch)
    await vi.advanceTimersByTimeAsync(30 * MIN)
    const value = await result

    expect(isDownloadsBlocked(value)).toBe(true)
    expect(value).toEqual({ blocked: true, blockedForMs: 25 * MIN })
    // 429s at 0, 1, 3, 7, 15 and 25 minutes; the last one crosses the 20-minute threshold.
    expect(doFetch).toHaveBeenCalledTimes(6)
    expect(log.warn).toHaveBeenCalledWith(
      expect.objectContaining({ egress: 'direct', blockedForMs: 25 * MIN }),
      'npm downloads blocked, yielding lane',
    )
  })

  it('probes once and yields again when called later during the same block', async () => {
    const throttle = createDownloadsThrottle(FLOOR, makeLog())
    const doFetch = vi.fn(async () => rateLimited)

    const first = throttle.run('direct', doFetch)
    await vi.advanceTimersByTimeAsync(30 * MIN)
    await first
    doFetch.mockClear()

    await vi.advanceTimersByTimeAsync(60 * MIN)
    const second = throttle.run('direct', doFetch)
    await vi.advanceTimersByTimeAsync(0)

    expect(isDownloadsBlocked(await second)).toBe(true)
    expect(doFetch).toHaveBeenCalledTimes(1)
  })

  it('treats plain values as not blocked', () => {
    expect(isDownloadsBlocked({ count: 1 })).toBe(false)
    expect(isDownloadsBlocked(rateLimited)).toBe(false)
  })
})

describe('floor above the 60 second cap', () => {
  it('keeps the interval at the floor after a 429', () => {
    const throttle = createDownloadsThrottle(90_000, makeLog())

    throttle.recordRateLimit('direct')

    expect(throttle.intervalMs('direct')).toBe(90_000)
  })
})

describe('cancellation', () => {
  it('rejects a paused run on abort without fetching or reserving a slot', async () => {
    const throttle = createDownloadsThrottle(FLOOR, makeLog())
    throttle.recordRateLimit('direct')
    const controller = new AbortController()
    const doFetch = vi.fn(async () => ({ count: 1 }))
    const outcome = throttle.run('direct', doFetch, undefined, controller.signal).then(
      () => 'resolved',
      () => 'rejected',
    )

    await vi.advanceTimersByTimeAsync(10_000)
    controller.abort()
    await vi.advanceTimersByTimeAsync(30_000)

    await expect(outcome).resolves.toBe('rejected')
    expect(doFetch).not.toHaveBeenCalled()

    // The pause from the 429 ends 20s from here; an aborted run must not have taken a slot.
    const { times, track } = settleTimes(Date.now())
    track(throttle.acquire('direct'))
    await vi.advanceTimersByTimeAsync(20_000)
    expect(times).toEqual([20_000])
  })

  it('rejects at once when the signal is already aborted', async () => {
    const throttle = createDownloadsThrottle(FLOOR, makeLog())
    const doFetch = vi.fn(async () => ({ count: 1 }))

    await expect(throttle.run('direct', doFetch, undefined, AbortSignal.abort())).rejects.toThrow()
    expect(doFetch).not.toHaveBeenCalled()
  })
})
