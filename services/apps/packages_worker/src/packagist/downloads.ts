import {
  insertLast30dDownloadIfAbsent,
  logAuditFieldChanges,
} from '@crowd/data-access-layer/src/packages'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

const WORKER = 'packagist'

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
// the value is the observation closest to the boundary. insertLast30dDownloadIfAbsent
// does the presence check and the insert (+ mirror) atomically in one statement, so
// concurrent runs for the same purl+month can't both pass a check-then-insert gap and
// have the later one clobber the earlier one's count. The audit record shares the same
// transaction, so a failed audit insert can never leave a committed window unaudited.
export async function persistPackagist30dWindow(
  qx: QueryExecutor,
  purl: string,
  monthly: number | null,
  runDate: string,
): Promise<string[]> {
  if (monthly == null) return []

  const { startDate, endDate } = monthlyWindowFor(runDate)
  return qx.tx(async (t) => {
    const changedFields = await insertLast30dDownloadIfAbsent(
      t,
      purl,
      startDate,
      endDate,
      monthly,
      true,
    )
    await logAuditFieldChanges(t, WORKER, purl, changedFields)
    return changedFields
  })
}
