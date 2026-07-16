import {
  getExistingLast30dEndDates,
  upsertLast30dDownload,
} from '@crowd/data-access-layer/src/packages'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

// Compute the monthly window for observed rolling 30d downloads.
// endDate = first of the run month; startDate = endDate minus 30 days.
export function monthlyWindowFor(runDate: string): { startDate: string; endDate: string } {
  const d = new Date(runDate + 'T00:00:00Z')
  // endDate: first of the current month
  const endDate = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1))
  // startDate: endDate minus 30 days
  const startDate = new Date(endDate.getTime() - 30 * 24 * 60 * 60 * 1000)

  return {
    startDate: startDate.toISOString().slice(0, 10),
    endDate: endDate.toISOString().slice(0, 10),
  }
}

// One window row per purl per month, mirrored to packages.downloads_last_30d
// (npm parity). A window already recorded for the month is never overwritten —
// the value is the observation closest to the boundary.
export async function persistPackagist30dWindow(
  qx: QueryExecutor,
  purl: string,
  monthly: number | null,
  runDate: string,
): Promise<void> {
  if (monthly == null) return

  const { startDate, endDate } = monthlyWindowFor(runDate)
  const existing = await getExistingLast30dEndDates(qx, purl, endDate, endDate)
  if (existing.length === 0) {
    await upsertLast30dDownload(qx, purl, startDate, endDate, monthly, true)
  }
}
