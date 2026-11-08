import { afterEach, describe, expect, it, vi } from 'vitest'

import { BatchProcessor } from './utils'

function gate() {
  let open!: () => void
  const opened = new Promise<void>((resolve) => {
    open = resolve
  })
  return { opened, open }
}

const flush = () => new Promise((resolve) => setImmediate(resolve))

describe('BatchProcessor', () => {
  afterEach(() => {
    vi.useRealTimers()
  })

  it('runs one batch at a time and starts the next as soon as the previous one ends', async () => {
    const gates = Array.from({ length: 5 }, gate)
    const started: number[][] = []
    let running = 0
    let peak = 0

    const processor = new BatchProcessor<number>(
      2,
      60,
      async (batch) => {
        const gateForBatch = gates[started.length]
        started.push(batch)
        running++
        peak = Math.max(peak, running)
        await gateForBatch.opened
        running--
      },
      async (_, err) => {
        throw err
      },
    )

    const callers = Array.from({ length: 10 }, (_, i) => processor.addToBatch(i))
    await flush()
    expect(started).toEqual([[0, 1]])

    for (let i = 0; i < 4; i++) {
      gates[i].open()
      await flush()
      expect(started).toHaveLength(i + 2)
    }
    gates[4].open()
    await Promise.all(callers)

    expect(started).toEqual([
      [0, 1],
      [2, 3],
      [4, 5],
      [6, 7],
      [8, 9],
    ])
    expect(peak).toBe(1)
  })

  it('gives a failed batch error only to the caller that started the batch', async () => {
    const failingBatch = gate()
    const processed: string[] = []

    const processor = new BatchProcessor<string>(
      1,
      60,
      async (batch) => {
        if (batch[0] === 'bad') {
          await failingBatch.opened
          throw new Error('boom')
        }
        processed.push(...batch)
      },
      async (_, err) => {
        throw err
      },
    )

    const starter = processor.addToBatch('bad')
    const waiter = processor.addToBatch('ok')
    failingBatch.open()

    await expect(starter).rejects.toThrow('boom')
    await expect(waiter).resolves.toBeUndefined()
    expect(processed).toEqual(['ok'])
  })

  it('flushes a partial batch when its timeout passes', async () => {
    vi.useFakeTimers()
    const processed: number[][] = []
    const processor = new BatchProcessor<number>(
      10,
      1,
      async (batch) => {
        processed.push(batch)
      },
      async () => undefined,
    )

    await processor.addToBatch(1)
    await processor.addToBatch(2)
    expect(processed).toEqual([])

    await vi.advanceTimersByTimeAsync(1000)
    expect(processed).toEqual([[1, 2]])
  })

  it('flushes what is left on stop and then rejects new elements', async () => {
    const processed: number[][] = []
    const processor = new BatchProcessor<number>(
      10,
      60,
      async (batch) => {
        processed.push(batch)
      },
      async () => undefined,
    )

    await processor.addToBatch(1)
    await processor.addToBatch(2)
    await processor.stop()

    expect(processed).toEqual([[1, 2]])
    await expect(processor.addToBatch(3)).rejects.toThrow('Batch processor is stopped')
  })
})
