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
  let firstError: unknown
  let failed = false

  for (let i = 0; i < items.length && !failed; i++) {
    const idx = i
    const p: Promise<void> = fn(items[idx])
      .then((r) => {
        results[idx] = r
      })
      .catch((err) => {
        if (!failed) {
          failed = true
          firstError = err
        }
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
  if (failed) throw firstError
  return results
}
