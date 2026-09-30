// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { describe, expect, it } from 'vitest'

import { settlePool } from './settlePool'

const deferred = <T>() => {
  let resolve!: (value: T) => void
  let reject!: (reason: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

const tick = () => new Promise((resolve) => setTimeout(resolve, 0))

describe('settlePool', () => {
  it('never runs more than the limit at once and keeps input order', async () => {
    let inFlight = 0
    let peak = 0

    const results = await settlePool([1, 2, 3, 4, 5, 6, 7], 3, async (n) => {
      inFlight++
      peak = Math.max(peak, inFlight)
      await tick()
      inFlight--
      return n * 2
    })

    expect(peak).toBe(3)
    expect(results.map((r) => (r.status === 'fulfilled' ? r.value : null))).toEqual([
      2, 4, 6, 8, 10, 12, 14,
    ])
  })

  it('starts the next item when one finishes instead of waiting for the slowest', async () => {
    const gates = [deferred<string>(), deferred<string>(), deferred<string>(), deferred<string>()]
    const started: number[] = []

    const pool = settlePool([0, 1, 2, 3], 2, (i) => {
      started.push(i)
      return gates[i].promise
    })

    await tick()
    expect(started).toEqual([0, 1])

    gates[1].resolve('fast')
    await tick()
    expect(started).toEqual([0, 1, 2])

    gates[2].resolve('fast')
    await tick()
    expect(started).toEqual([0, 1, 2, 3])

    gates[0].resolve('slow')
    gates[3].resolve('last')
    expect((await pool).map((r) => r.status)).toEqual([
      'fulfilled',
      'fulfilled',
      'fulfilled',
      'fulfilled',
    ])
  })

  it('records a rejection and keeps going', async () => {
    const results = await settlePool([1, 2, 3], 2, async (n) => {
      if (n === 2) {
        throw new Error('boom')
      }
      return n
    })

    expect(results.map((r) => r.status)).toEqual(['fulfilled', 'rejected', 'fulfilled'])
  })

  it('stops starting new items after a fatal rejection and lets in-flight ones settle', async () => {
    const started: number[] = []
    const fatal = new Error('cancelled')

    const results = await settlePool(
      [1, 2, 3, 4, 5],
      2,
      async (n) => {
        started.push(n)
        await tick()
        if (n === 1) {
          throw fatal
        }
        return n
      },
      (reason) => reason === fatal,
    )

    expect(started).toEqual([1, 2])
    expect(results.map((r) => r.status)).toEqual(['rejected', 'fulfilled'])
  })

  it('returns nothing for no items', async () => {
    expect(await settlePool([], 4, async () => 1)).toEqual([])
  })
})
