import path from 'node:path'

import { describe, expect, test, vi } from 'vitest'

import { MAX_CONCURRENT_WORKERS, runInWorker } from './runChecksIsolated'

const threads = vi.hoisted(() => ({ alive: 0, maxAlive: 0, created: 0 }))

vi.mock('node:worker_threads', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:worker_threads')>()

  class CountedWorker extends actual.Worker {
    constructor(...args: ConstructorParameters<typeof actual.Worker>) {
      super(...args)
      threads.created++
      threads.alive++
      threads.maxAlive = Math.max(threads.maxAlive, threads.alive)
      this.once('exit', () => {
        threads.alive--
      })
    }
  }

  return { ...actual, Worker: CountedWorker }
})

const busyLoop = path.join(__dirname, '__fixtures__', 'workers', 'busyLoop.mjs')

const sleeps = path.join(__dirname, '__fixtures__', 'workers', 'sleeps.mjs')

describe('runInWorker thread lifetime', () => {
  test('never has more than MAX_CONCURRENT_WORKERS threads alive, killed ones included', async () => {
    const killed = { timeoutMs: 100, timeoutMessage: 'timed out', maxOldGenerationSizeMb: 64 }
    const patient = { ...killed, timeoutMs: 10_000 }

    const results = await Promise.allSettled([
      ...Array.from({ length: MAX_CONCURRENT_WORKERS }, () => runInWorker(busyLoop, {}, killed)),
      ...Array.from({ length: MAX_CONCURRENT_WORKERS }, () =>
        runInWorker(sleeps, { ms: 20 }, patient),
      ),
    ])

    expect(results.filter((r) => r.status === 'fulfilled')).toHaveLength(MAX_CONCURRENT_WORKERS)
    expect(threads.maxAlive).toBe(MAX_CONCURRENT_WORKERS)
    expect(threads.alive).toBe(0)
  })

  test('does not start a thread when no time is left', async () => {
    const before = threads.created

    await expect(
      runInWorker(
        sleeps,
        { ms: 20 },
        { timeoutMs: 0, timeoutMessage: 'x', maxOldGenerationSizeMb: 64 },
      ),
    ).rejects.toThrow('x')

    expect(threads.created).toBe(before)
  })
})
