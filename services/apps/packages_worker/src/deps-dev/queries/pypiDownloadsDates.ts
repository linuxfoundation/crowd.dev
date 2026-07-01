// Date math for the PyPI downloads workflows. Pure functions — no clock access; the
// caller passes the reference "today" so runs are deterministic and testable.

// PyPI's BigQuery download data is unreliable before this date: Linehaul under-reported
// downloads prior to 2018-07-26 (see the official PyPI download-analysis guide). Backfills
// are clamped to this floor so we never ingest known-bad counts.
export const PYPI_EARLIEST = '2018-07-26'

export interface Last30dWindow {
  start: string
  end: string
  isLatest: boolean
}

function addDaysUTC(date: string, days: number): string {
  const d = new Date(date + 'T00:00:00Z')
  d.setUTCDate(d.getUTCDate() + days)
  return d.toISOString().slice(0, 10)
}

// 1st of the calendar month (UTC) containing `today` (YYYY-MM-DD).
export function utcFirstOfCurrentMonth(today: string): string {
  const d = new Date(today + 'T00:00:00Z')
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1)).toISOString().slice(0, 10)
}

// Monthly 30-day windows, identical in math to npm's computeMissingLast30dWindows:
// end_date = 1st of a calendar month (UTC), start_date = end_date - 30 days, isLatest only
// for the bucket whose end === upperEndDate. Walks from max(fromDate, PYPI_EARLIEST) up to
// upperEndDate. No fromDate → only the latest bucket.
export function computeLast30dWindows(
  fromDate: string | null,
  upperEndDate: string,
): Last30dWindow[] {
  const lower = fromDate ? (fromDate > PYPI_EARLIEST ? fromDate : PYPI_EARLIEST) : upperEndDate
  const lowerDate = new Date(lower + 'T00:00:00Z')
  const firstMonth = Date.UTC(lowerDate.getUTCFullYear(), lowerDate.getUTCMonth(), 1)
  const lastMonth = new Date(upperEndDate + 'T00:00:00Z').getTime()
  if (firstMonth > lastMonth) return []

  const result: Last30dWindow[] = []
  let m = firstMonth
  while (m <= lastMonth) {
    const d = new Date(m)
    const endDate = d.toISOString().slice(0, 10) // 1st of the month
    // Skip windows whose end precedes the Linehaul floor.
    if (endDate >= PYPI_EARLIEST) {
      let start = addDaysUTC(endDate, -30)
      if (start < PYPI_EARLIEST) start = PYPI_EARLIEST
      result.push({ start, end: endDate, isLatest: m === lastMonth })
    }
    m = Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 1)
  }
  return result
}

// The daily trailing window spans 2 days so the most-recent (possibly partial) partition is
// re-scanned once on the next run and corrected, while keeping the daily BigQuery scan small.
const DAILY_TRAILING_DAYS = 2

// Default daily trailing window: [today - 2, today - 1] inclusive (self-healing).
export function defaultDailyRange(today: string): { startDate: string; endDate: string } {
  return { startDate: addDaysUTC(today, -DAILY_TRAILING_DAYS), endDate: addDaysUTC(today, -1) }
}
