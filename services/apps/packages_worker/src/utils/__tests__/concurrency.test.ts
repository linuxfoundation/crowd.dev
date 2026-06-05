import { describe, expect, it } from 'vitest'

import { mapWithConcurrency } from '../concurrency'

const tick = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))

describe('mapWithConcurrency', () => {
  it('preserves input order regardless of completion order', async () => {
    const items = [1, 2, 3, 4, 5]
    // Later items finish first; output must still be in input order.
    const out = await mapWithConcurrency(items, 2, async (n) => {
      await tick((6 - n) * 5)
      return n * 10
    })
    expect(out).toEqual([10, 20, 30, 40, 50])
  })

  it('never exceeds the concurrency cap (and does run in parallel)', async () => {
    let inFlight = 0
    let max = 0
    await mapWithConcurrency([...Array(10).keys()], 3, async (n) => {
      inFlight++
      max = Math.max(max, inFlight)
      await tick(5)
      inFlight--
      return n
    })
    expect(max).toBeLessThanOrEqual(3)
    expect(max).toBeGreaterThan(1)
  })

  it('throws for non-positive or non-integer concurrency', async () => {
    await expect(mapWithConcurrency([1], 0, async (x) => x)).rejects.toThrow(/positive integer/)
    await expect(mapWithConcurrency([1], -1, async (x) => x)).rejects.toThrow()
    await expect(mapWithConcurrency([1], 1.5, async (x) => x)).rejects.toThrow()
  })

  it('propagates errors from the mapper', async () => {
    await expect(
      mapWithConcurrency([1, 2, 3], 2, async (n) => {
        if (n === 2) throw new Error('boom')
        return n
      }),
    ).rejects.toThrow('boom')
  })

  it('returns an empty array for empty input', async () => {
    expect(await mapWithConcurrency<number, number>([], 2, async (x) => x)).toEqual([])
  })
})
