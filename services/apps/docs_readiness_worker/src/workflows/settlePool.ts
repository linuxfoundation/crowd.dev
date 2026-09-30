// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

// Runs `run` over `items` with at most `limit` in flight, starting the next item as soon as one settles.
export async function settlePool<T, R>(
  items: readonly T[],
  limit: number,
  run: (item: T) => Promise<R>,
  stopOn: (reason: unknown) => boolean = () => false,
): Promise<PromiseSettledResult<R>[]> {
  const results: PromiseSettledResult<R>[] = []
  let next = 0
  let stopped = false

  const worker = async (): Promise<void> => {
    while (!stopped && next < items.length) {
      const index = next++
      try {
        results[index] = { status: 'fulfilled', value: await run(items[index]) }
      } catch (reason) {
        results[index] = { status: 'rejected', reason }
        stopped = stopOn(reason)
      }
    }
  }

  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker))

  return results.filter(() => true)
}
