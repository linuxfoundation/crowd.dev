export const LATE_SYNC_GRACE_PERIOD_MS = 3 * 24 * 60 * 60 * 1000

export interface INangoWindowRecord {
  timestamp: number
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
  const earlyExitThresholdMs = windowEndMs + LATE_SYNC_GRACE_PERIOD_MS
  const result: T[] = []
  let cursor: string | undefined
  let reachedEarlyExitThreshold = false

  // Nango's records API guarantees pages are ordered by lastModifiedAt ascending
  // (https://docs.nango.dev/reference/api/sync/records-list), so it's safe to stop
  // once a record crosses the threshold.
  while (!reachedEarlyExitThreshold) {
    const page = await fetchPage(cursor)

    for (const record of page.records) {
      const lastModifiedAtMs = new Date(record.metadata.lastModifiedAt).getTime()
      if (lastModifiedAtMs >= earlyExitThresholdMs) {
        reachedEarlyExitThreshold = true
        break
      }
      if (record.timestamp >= windowStartMs && record.timestamp < windowEndMs) {
        result.push(record)
      }
    }

    if (reachedEarlyExitThreshold || !page.nextCursor) {
      break
    }
    cursor = page.nextCursor
  }

  return result
}
