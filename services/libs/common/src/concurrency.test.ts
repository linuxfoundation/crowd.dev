import { describe, expect, it } from 'vitest'

import { mapWithConcurrency } from './concurrency'

describe('mapWithConcurrency', () => {
  it('keeps the results in input order', async () => {
    const delays = [30, 5, 20, 1]

    const results = await mapWithConcurrency(delays, 2, async (delay, index) => {
      await new Promise((resolve) => setTimeout(resolve, delay))
      return `${index}:${delay}`
    })

    expect(results).toEqual(['0:30', '1:5', '2:20', '3:1'])
  })

  it('never runs more calls at once than the limit', async () => {
    let running = 0
    let peak = 0

    await mapWithConcurrency(
      Array.from({ length: 12 }, (_, i) => i),
      3,
      async () => {
        running++
        peak = Math.max(peak, running)
        await new Promise((resolve) => setTimeout(resolve, 2))
        running--
      },
    )

    expect(peak).toBe(3)
  })

  it('returns an empty list for no items and rejects an invalid limit', async () => {
    await expect(mapWithConcurrency([], 4, async () => 1)).resolves.toEqual([])
    await expect(mapWithConcurrency([1], 0, async () => 1)).rejects.toThrow('at least 1')
  })

  it('propagates the first failure and starts no further item after it', async () => {
    const started: number[] = []

    await expect(
      mapWithConcurrency([1, 2, 3, 4], 2, async (item) => {
        started.push(item)
        if (item === 1) {
          throw new Error('boom')
        }
        await new Promise((resolve) => setTimeout(resolve, 5))
        return item
      }),
    ).rejects.toThrow('boom')

    expect(started).toEqual([1, 2])
  })
})
