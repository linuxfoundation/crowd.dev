export async function mapWithConcurrency<T, R>(
  items: T[],
  concurrency: number,
  fn: (item: T) => Promise<R>,
): Promise<R[]> {
  const results: R[] = new Array(items.length)
  const executing = new Set<Promise<void>>()

  for (let i = 0; i < items.length; i++) {
    const idx = i
    const p: Promise<void> = fn(items[idx]).then((r) => {
      results[idx] = r
    })
    executing.add(p)
    void p.finally(() => executing.delete(p))

    if (executing.size >= concurrency) {
      await Promise.race(executing)
    }
  }

  await Promise.all(executing)
  return results
}
