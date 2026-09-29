import path from 'node:path'

import { describe, expect, test } from 'vitest'

import { DeadlineExceededError } from './deadlineError'
import { MAX_CONCURRENT_WORKERS, runChecksIsolated, runInWorker } from './runChecksIsolated'

const fixture = (name: string) => path.join(__dirname, '__fixtures__', 'workers', name)

const options = { timeoutMs: 10_000, timeoutMessage: 'timed out', maxOldGenerationSizeMb: 64 }

describe('runInWorker', () => {
  test('resolves with the worker result and hands it the worker data', async () => {
    await expect(runInWorker(fixture('returns.mjs'), { url: 'x' }, options)).resolves.toEqual({
      echoed: { url: 'x' },
    })
  })

  test('rejects with the error message the worker reports', async () => {
    await expect(runInWorker(fixture('failsWithMessage.mjs'), {}, options)).rejects.toThrow(
      'boom from worker',
    )
  })

  test('rejects, and does not resolve undefined, when the worker reports an empty error', async () => {
    await expect(runInWorker(fixture('failsWithEmptyMessage.mjs'), {}, options)).rejects.toThrow(
      'failed without an error message',
    )
  })

  test('rejects when the worker throws', async () => {
    await expect(runInWorker(fixture('throws.mjs'), {}, options)).rejects.toThrow(
      'uncaught in worker',
    )
  })

  test('rejects when the worker exits without a result', async () => {
    await expect(runInWorker(fixture('exitsSilently.mjs'), {}, options)).rejects.toThrow(
      'exited with code 3',
    )
  })

  test('kills a worker stuck in a synchronous loop and keeps the main thread responsive', async () => {
    let lastTick = Date.now()
    let maxGapMs = 0
    const heartbeat = setInterval(() => {
      const now = Date.now()
      maxGapMs = Math.max(maxGapMs, now - lastTick)
      lastTick = now
    }, 20)
    const startedAt = Date.now()

    await expect(
      runInWorker(fixture('busyLoop.mjs'), {}, { ...options, timeoutMs: 300 }),
    ).rejects.toThrow('timed out')

    clearInterval(heartbeat)
    expect(Date.now() - startedAt).toBeLessThan(3000)
    expect(maxGapMs).toBeLessThan(500)
  })

  test('a worker that outlives its deadline rejects with DeadlineExceededError', async () => {
    const error = await runInWorker(
      fixture('busyLoop.mjs'),
      {},
      { ...options, timeoutMs: 200 },
    ).catch((err: Error) => err)

    expect(error).toBeInstanceOf(DeadlineExceededError)
  })

  test('a worker that fails on its own is not a DeadlineExceededError', async () => {
    const error = await runInWorker(fixture('throws.mjs'), {}, options).catch((err: Error) => err)

    expect(error).toBeInstanceOf(Error)
    expect(error).not.toBeInstanceOf(DeadlineExceededError)
  })

  test('stops a spinning thread once the deadline passes', async () => {
    const counters = new SharedArrayBuffer(8)
    const ticks = new Int32Array(counters)

    await expect(
      runInWorker(fixture('spinsAndCounts.mjs'), { counters }, { ...options, timeoutMs: 300 }),
    ).rejects.toThrow('timed out')

    const atRejection = Atomics.load(ticks, 0)
    await new Promise((resolve) => setTimeout(resolve, 150))
    expect(atRejection).toBeGreaterThan(0)
    expect(Atomics.load(ticks, 0)).toBe(atRejection)
  })

  test('a worker that hits its memory limit fails without taking the process down', async () => {
    await expect(
      runInWorker(fixture('allocatesUntilLimit.mjs'), {}, { ...options, timeoutMs: 30_000 }),
    ).rejects.toThrow(/memory/i)
  })

  describe('concurrency cap', () => {
    test('never runs more than MAX_CONCURRENT_WORKERS threads at once, and still finishes them all', async () => {
      const counters = new SharedArrayBuffer(8)
      const total = MAX_CONCURRENT_WORKERS * 2 + 1

      const results = await Promise.all(
        Array.from({ length: total }, () =>
          runInWorker(fixture('holdsSlot.mjs'), { counters }, options),
        ),
      )

      const maxSeen = Atomics.load(new Int32Array(counters), 1)
      expect(results).toEqual(Array(total).fill('held'))
      expect(maxSeen).toBeGreaterThan(1)
      expect(maxSeen).toBeLessThanOrEqual(MAX_CONCURRENT_WORKERS)
    })

    test('a call that waits for a slot past its deadline rejects without starting a thread', async () => {
      const busy = Array.from({ length: MAX_CONCURRENT_WORKERS }, () =>
        runInWorker(fixture('busyLoop.mjs'), {}, { ...options, timeoutMs: 600 }).catch(() => null),
      )
      const counters = new SharedArrayBuffer(8)
      const startedAt = Date.now()

      const error = await runInWorker(
        fixture('holdsSlot.mjs'),
        { counters },
        { ...options, timeoutMs: 150, timeoutMessage: 'queued too long' },
      ).catch((err: Error) => err)

      expect(error).toBeInstanceOf(DeadlineExceededError)
      expect((error as Error).message).toBe(
        'queued too long while waiting for a free scoring thread',
      )

      expect(Date.now() - startedAt).toBeLessThan(500)
      expect(Atomics.load(new Int32Array(counters), 0)).toBe(0)
      expect(Atomics.load(new Int32Array(counters), 1)).toBe(0)

      await Promise.all(busy)
      await expect(runInWorker(fixture('returns.mjs'), { ok: 1 }, options)).resolves.toEqual({
        echoed: { ok: 1 },
      })
    })

    test('the time spent waiting for a slot counts against the same deadline', async () => {
      const holders = Array.from({ length: MAX_CONCURRENT_WORKERS }, () =>
        runInWorker(fixture('sleeps.mjs'), { ms: 500 }, options),
      )
      const startedAt = Date.now()

      await expect(
        runInWorker(fixture('busyLoop.mjs'), {}, { ...options, timeoutMs: 1000 }),
      ).rejects.toThrow('timed out')

      const elapsed = Date.now() - startedAt
      expect(elapsed).toBeGreaterThanOrEqual(900)
      expect(elapsed).toBeLessThan(1350)
      await Promise.all(holders)
    })

    test('a waiter that times out removes only itself from the queue', async () => {
      const busy = Array.from({ length: MAX_CONCURRENT_WORKERS }, () =>
        runInWorker(fixture('busyLoop.mjs'), {}, { ...options, timeoutMs: 700 }).catch(() => null),
      )

      const [late, early] = await Promise.allSettled([
        runInWorker(fixture('returns.mjs'), { who: 'late' }, { ...options, timeoutMs: 5000 }),
        runInWorker(fixture('returns.mjs'), { who: 'early' }, { ...options, timeoutMs: 150 }),
      ])

      expect(early.status).toBe('rejected')
      expect(late).toEqual({ status: 'fulfilled', value: { echoed: { who: 'late' } } })
      await Promise.all(busy)
    })
  })
})

describe('runInWorker without time left', () => {
  test('rejects with the timeout message and still frees its slot', async () => {
    const error = await runInWorker(fixture('returns.mjs'), {}, { ...options, timeoutMs: 0 }).catch(
      (err: Error) => err,
    )

    expect(error).toBeInstanceOf(DeadlineExceededError)
    expect((error as Error).message).toContain('timed out')

    await expect(runInWorker(fixture('returns.mjs'), { ok: 1 }, options)).resolves.toEqual({
      echoed: { ok: 1 },
    })
  })
})

describe('runChecksIsolated', () => {
  test('loads afdocs inside the worker thread and reports its errors', async () => {
    await expect(runChecksIsolated('not a url', 10_000, 'timed out')).rejects.toThrow(/Invalid URL/)
  })
})
