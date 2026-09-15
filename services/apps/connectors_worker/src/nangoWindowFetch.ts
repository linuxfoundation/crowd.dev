export interface INangoWindowRecord {
  metadata: { lastModifiedAt: string }
}

export interface INangoWindowPage<T extends INangoWindowRecord> {
  records: T[]
  nextCursor?: string
}

export type FetchNangoPage<T extends INangoWindowRecord> = (
  cursor?: string,
) => Promise<INangoWindowPage<T>>

export async function fetchNangoRecordsInWindow<T extends INangoWindowRecord>(
  fetchPage: FetchNangoPage<T>,
  windowStart: Date,
  windowEnd: Date,
): Promise<T[]> {
  const windowStartMs = windowStart.getTime()
  const windowEndMs = windowEnd.getTime()
  const result: T[] = []
  let cursor: string | undefined
  let reachedWindowEnd = false

  while (!reachedWindowEnd) {
    const page = await fetchPage(cursor)

    for (const record of page.records) {
      const modifiedAtMs = new Date(record.metadata.lastModifiedAt).getTime()
      if (modifiedAtMs >= windowEndMs) {
        reachedWindowEnd = true
        break
      }
      if (modifiedAtMs >= windowStartMs) {
        result.push(record)
      }
    }

    if (reachedWindowEnd || !page.nextCursor) {
      break
    }
    cursor = page.nextCursor
  }

  return result
}
