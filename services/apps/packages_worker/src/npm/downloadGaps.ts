export const NPM_EARLIEST = '2015-01-10'
const MAX_RANGE_DAYS = 540
// npm caps a bulk query at 128 unscoped packages and 365 days.
export const BULK_MAX_NAMES = 128
export const BULK_MAX_SPAN_DAYS = 365

export function addDays(day: string, n: number): string {
  const d = new Date(`${day}T00:00:00Z`)
  d.setUTCDate(d.getUTCDate() + n)
  return d.toISOString().slice(0, 10)
}

export function daysBetween(start: string, end: string): string[] {
  const out: string[] = []
  for (let d = start; d <= end; d = addDays(d, 1)) out.push(d)
  return out
}

// Cover sorted YYYY-MM-DD dates with as few ranges as possible, each spanning at most maxDays.
// Stored days inside a range are refetched, which costs less than another request.
export function coverDates(
  dates: string[],
  maxDays: number = MAX_RANGE_DAYS,
): Array<{ start: string; end: string }> {
  const ranges: Array<{ start: string; end: string }> = []
  for (const day of dates) {
    const last = ranges[ranges.length - 1]
    if (last && daysDiff(last.start, day) < maxDays) last.end = day
    else ranges.push({ start: day, end: day })
  }
  return ranges
}

function daysDiff(a: string, b: string): number {
  return (Date.parse(b) - Date.parse(a)) / 86_400_000
}
