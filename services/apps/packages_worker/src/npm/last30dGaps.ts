import { NPM_EARLIEST } from './downloadGaps'

export interface Last30dWindow {
  start: string
  end: string
  isLatest: boolean
}

// Compute the monthly 30-day windows that are missing from downloads_last_30d.
// Each window: end_date = 1st of a calendar month, start_date = end_date - 30 days.
// upperEndDate must be the 1st of the current UTC month — callers derive it via
// utcFirstOfCurrentMonth() so the cutoff is unambiguous.
export function computeMissingLast30dWindows(
  firstReleaseAt: string | null,
  upperEndDate: string,
  existingEndDates: string[],
): Last30dWindow[] {
  const lower =
    firstReleaseAt && firstReleaseAt.slice(0, 10) > NPM_EARLIEST
      ? firstReleaseAt.slice(0, 10)
      : NPM_EARLIEST

  const lowerDate = new Date(lower + 'T00:00:00Z')
  const firstMonth = Date.UTC(lowerDate.getUTCFullYear(), lowerDate.getUTCMonth(), 1)

  const lastMonth = new Date(upperEndDate + 'T00:00:00Z').getTime()

  if (firstMonth > lastMonth) return []

  const existing = new Set(existingEndDates)
  const result: Last30dWindow[] = []

  let m = firstMonth
  while (m <= lastMonth) {
    const d = new Date(m)
    const year = d.getUTCFullYear()
    const month = d.getUTCMonth()

    const endDate = d.toISOString().slice(0, 10) // 1st of month

    if (endDate >= NPM_EARLIEST && !existing.has(endDate)) {
      const startDay = new Date(d)
      startDay.setUTCDate(startDay.getUTCDate() - 30)
      const start = startDay.toISOString().slice(0, 10)
      result.push({
        start: start < NPM_EARLIEST ? NPM_EARLIEST : start,
        end: endDate,
        isLatest: m === lastMonth,
      })
    }

    m = Date.UTC(year, month + 1, 1)
  }

  return result
}
