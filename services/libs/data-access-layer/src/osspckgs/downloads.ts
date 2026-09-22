import { insertDailyDownloads } from '../packages/downloadsDaily'
import { upsertLast30dDownload } from '../packages/downloadsLast30d'
import { QueryExecutor } from '../queryExecutor'

export async function recordDownloadSnapshot(
  qx: QueryExecutor,
  params: {
    packageId: number
    purl: string
    totalDownloads: number
    today: string // YYYY-MM-DD
  },
): Promise<string[]> {
  const { packageId, purl, totalDownloads, today } = params
  const changed: string[] = []

  const prevRow = await qx.selectOne(
    `SELECT total_downloads FROM packages WHERE id = $(packageId)`,
    { packageId },
  )
  const prev: number | null =
    prevRow?.total_downloads != null ? Number(prevRow.total_downloads) : null

  if (prev !== null && totalDownloads > prev) {
    const delta = totalDownloads - prev
    const dailyChanged = await insertDailyDownloads(qx, String(packageId), [
      { day: today, downloads: delta },
    ])
    dailyChanged.forEach((f) => changed.push(f))
  }

  const updated = await qx.result(
    `UPDATE packages SET total_downloads = $(totalDownloads)
      WHERE id = $(packageId)
        AND (total_downloads IS NULL OR total_downloads < $(totalDownloads))`,
    { totalDownloads, packageId },
  )
  if (updated > 0) changed.push('packages.total_downloads')

  const sumRow = await qx.selectOne(
    `SELECT COALESCE(SUM(count), 0)::bigint AS total
       FROM downloads_daily
      WHERE package_id = $(packageId)::bigint
        AND date > $(today)::date - INTERVAL '30 days'
        AND date <= $(today)::date`,
    { packageId, today },
  )
  const monthlySum = Number(sumRow.total)

  const thirtyDaysAgo = new Date(today)
  thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 29)
  const startDate = thirtyDaysAgo.toISOString().split('T')[0]

  const monthlyChanged = await upsertLast30dDownload(qx, purl, startDate, today, monthlySum, true)
  monthlyChanged.forEach((f) => changed.push(f))

  return changed
}
