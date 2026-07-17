import { TimeoutError, generateUUIDv4, timeout } from '@crowd/common'

import { acquireLock, releaseLock } from './mutex'
import { RedisClient } from './types'

export interface SingleFlightOptions {
  lockTtlSeconds: number
  waitTimeoutSeconds: number
}

// 100-300ms jittered, matching mutex.ts's own retry spacing.
const randomPollDelayMs = () => Math.floor(Math.random() * 200) + 100

/**
 * Ensures only one caller per `lockKey` actually runs `compute()` at a time. A caller
 * that loses the initial race polls `readCached()` and retries a non-blocking lock
 * acquisition every ~100-300ms — so it returns as soon as either the holder finishes
 * and populates the cache, or the lock frees up (holder crashed/released), without
 * waiting out the full timeout in either case. If Redis itself is unreachable, lock
 * acquisition fails open (proceeds unlocked) rather than failing the caller.
 *
 * `waitTimeoutSeconds` should be >= `lockTtlSeconds`: otherwise a waiter can give up
 * and run `compute()` unlocked while the original holder is still legitimately working
 * within its own TTL window, reproducing the exact stampede this helper exists to avoid.
 */
export async function withSingleFlight<T>(
  client: RedisClient,
  lockKey: string,
  { lockTtlSeconds, waitTimeoutSeconds }: SingleFlightOptions,
  readCached: () => Promise<T | null>,
  compute: () => Promise<T>,
): Promise<T> {
  const token = generateUUIDv4()
  const deadline = Date.now() + waitTimeoutSeconds * 1000
  let holdsLock = false

  for (;;) {
    try {
      await acquireLock(client, lockKey, token, lockTtlSeconds, 0)
      holdsLock = true
      break
    } catch (error) {
      if (!(error instanceof TimeoutError)) {
        // Redis unavailable for locking — fail open, compute unprotected.
        break
      }
    }

    const cached = await readCached()
    if (cached !== null) {
      return cached
    }

    if (Date.now() >= deadline) {
      break
    }

    await timeout(randomPollDelayMs())
  }

  try {
    if (holdsLock) {
      // Whoever held the lock right before us may have finished and populated the
      // cache between our last poll and winning it — avoid a redundant compute.
      const cached = await readCached()
      if (cached !== null) {
        return cached
      }
    }

    return await compute()
  } finally {
    if (holdsLock) {
      await releaseLock(client, lockKey, token)
    }
  }
}
