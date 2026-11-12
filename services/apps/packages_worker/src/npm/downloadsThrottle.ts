import { parseEnvInt, timeout } from '@crowd/common'
import { getServiceChildLogger } from '@crowd/logging'

import { isClientError } from '../utils/isClientError'
import { type FetchError, FetchErrorKind, isFetchError } from './types'

const DEFAULT_FLOOR_MS = 1000
const MAX_INTERVAL_MS = 60_000
const FIRST_PAUSE_MS = 60_000
const MAX_PAUSE_MS = 10 * 60_000
const EASE_AFTER_MS = 10 * 60_000
const EASE_FACTOR = 0.9
const BLOCKED_YIELD_MS = 20 * 60_000
// Long waits are chunked so callers can heartbeat well inside the 2 minute heartbeat timeout.
const WAIT_CHUNK_MS = 30_000

export interface DownloadsBlocked {
  blocked: true
  blockedForMs: number
}

export interface ThrottleLogger {
  info: (obj: object, msg: string) => void
  warn: (obj: object, msg: string) => void
}

interface EgressState {
  intervalMs: number
  nextSlotAt: number
  pausedUntil: number
  consecutive: number
  firstRateLimitAt: number | null
  lastIntervalChangeAt: number
}

export function downloadsFloorMs(): number {
  return parseEnvInt(
    process.env.CROWD_PACKAGES_DOWNLOAD_SLEEP_MS,
    DEFAULT_FLOOR_MS,
    1,
    Number.MAX_SAFE_INTEGER,
  )
}

export function isDownloadsBlocked(v: unknown): v is DownloadsBlocked {
  return typeof v === 'object' && v !== null && (v as Partial<DownloadsBlocked>).blocked === true
}

export interface DownloadsThrottle {
  intervalMs(egress: string): number
  acquire(egress: string, onWait?: () => void, signal?: AbortSignal): Promise<void>
  recordSuccess(egress: string): void
  recordRateLimit(egress: string): number
  run<T>(
    egress: string,
    doFetch: () => Promise<T | FetchError>,
    onWait?: () => void,
    signal?: AbortSignal,
  ): Promise<T | FetchError | DownloadsBlocked>
}

// Paces api.npmjs.org downloads requests per egress IP. npm's 429s carry `retry-after: 0`, so the
// pause and spacing adapt to the 429s we observe instead of a server-stated window.
export function createDownloadsThrottle(floorMs: number, log: ThrottleLogger): DownloadsThrottle {
  const egresses = new Map<string, EgressState>()

  function state(egress: string): EgressState {
    let s = egresses.get(egress)
    if (!s) {
      s = {
        intervalMs: floorMs,
        nextSlotAt: 0,
        pausedUntil: 0,
        consecutive: 0,
        firstRateLimitAt: null,
        lastIntervalChangeAt: 0,
      }
      egresses.set(egress, s)
    }
    return s
  }

  function intervalMs(egress: string): number {
    return state(egress).intervalMs
  }

  // The slot check and the reservation run without an await in between, so concurrent callers on
  // one egress serialize. Each wakeup re-checks because another caller may have extended the pause.
  async function acquire(egress: string, onWait?: () => void, signal?: AbortSignal): Promise<void> {
    const s = state(egress)
    for (;;) {
      signal?.throwIfAborted()
      const waitMs = Math.max(s.nextSlotAt, s.pausedUntil) - Date.now()
      if (waitMs <= 0) break
      await timeout(Math.min(waitMs, WAIT_CHUNK_MS))
      onWait?.()
    }
    signal?.throwIfAborted()
    s.nextSlotAt = Date.now() + s.intervalMs
  }

  function recordSuccess(egress: string): void {
    const s = state(egress)
    const now = Date.now()
    s.consecutive = 0
    s.firstRateLimitAt = null
    if (s.intervalMs > floorMs && now - s.lastIntervalChangeAt >= EASE_AFTER_MS) {
      s.intervalMs = Math.max(floorMs, Math.round(s.intervalMs * EASE_FACTOR))
      s.lastIntervalChangeAt = now
      log.info({ egress, intervalMs: s.intervalMs }, 'npm downloads throttle eased')
    }
  }

  function recordRateLimit(egress: string): number {
    const s = state(egress)
    const now = Date.now()
    s.consecutive += 1
    const blockedSince = (s.firstRateLimitAt ??= now)
    s.intervalMs = Math.min(Math.max(MAX_INTERVAL_MS, floorMs), s.intervalMs * 2)
    s.lastIntervalChangeAt = now
    const pauseMs = Math.min(MAX_PAUSE_MS, FIRST_PAUSE_MS * 2 ** (s.consecutive - 1))
    s.pausedUntil = Math.max(s.pausedUntil, now + pauseMs)
    log.warn(
      { egress, intervalMs: s.intervalMs, pauseMs, consecutive: s.consecutive },
      'npm downloads rate limited',
    )
    return now - blockedSince
  }

  // Retries 429s after the egress pause and returns every other result unchanged. Once the egress
  // has seen only 429s for 20 minutes it yields, so the workflow can wait on a durable timer.
  async function run<T>(
    egress: string,
    doFetch: () => Promise<T | FetchError>,
    onWait?: () => void,
    signal?: AbortSignal,
  ): Promise<T | FetchError | DownloadsBlocked> {
    for (;;) {
      await acquire(egress, onWait, signal)
      onWait?.()
      signal?.throwIfAborted()
      const res = await doFetch()

      if (isFetchError(res) && res.kind === FetchErrorKind.RATE_LIMIT) {
        const blockedForMs = recordRateLimit(egress)
        if (blockedForMs >= BLOCKED_YIELD_MS) {
          log.warn({ egress, blockedForMs }, 'npm downloads blocked, yielding lane')
          return { blocked: true, blockedForMs }
        }
        continue
      }

      if (!isFetchError(res) || isClientError(res.statusCode, res.kind)) {
        recordSuccess(egress)
      }
      return res
    }
  }

  return { intervalMs, acquire, recordSuccess, recordRateLimit, run }
}

let shared: DownloadsThrottle | undefined

// One limiter per worker process, so the daily and 30-day flows share each egress's budget.
export function npmDownloadsThrottle(): DownloadsThrottle {
  shared ??= createDownloadsThrottle(downloadsFloorMs(), getServiceChildLogger('npm'))
  return shared
}
