export async function mapWithConcurrency<T, R>(
  items: T[],
  concurrency: number,
  fn: (item: T) => Promise<R>,
): Promise<R[]> {
  if (!Number.isInteger(concurrency) || concurrency < 1) {
    throw new Error(
      `mapWithConcurrency: concurrency must be a positive integer, got ${concurrency}`,
    )
  }

  const results: R[] = new Array(items.length)
  const executing = new Set<Promise<void>>()

  for (let i = 0; i < items.length; i++) {
    const idx = i
    // Chain the set-cleanup onto p itself (not a discarded p.finally()) so the
    // only promise we hold is the one awaited by race()/all() — a rejection
    // therefore never escapes as an unhandled rejection.
    const p: Promise<void> = fn(items[idx])
      .then((r) => {
        results[idx] = r
      })
      .finally(() => {
        executing.delete(p)
      })
    executing.add(p)

    if (executing.size >= concurrency) {
      await Promise.race(executing)
    }
  }

  await Promise.all(executing)
  return results
}
