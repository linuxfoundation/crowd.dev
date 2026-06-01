import { QueryExecutor } from '../queryExecutor'

export async function getMissingDownloadDates(
  qx: QueryExecutor,
  packageId: string,
  lower: string,
  yesterday: string,
): Promise<string[]> {
  const rows: Array<{ date: string }> = await qx.select(
    `WITH target AS (
       SELECT generate_series($(lower)::date, $(yesterday)::date, '1 day'::interval)::date AS d
     )
     SELECT d::text AS date FROM target
     EXCEPT
     SELECT date::text FROM downloads_daily
      WHERE package_id = $(packageId)::bigint
        AND date BETWEEN $(lower)::date AND $(yesterday)::date
     ORDER BY date`,
    { lower, yesterday, packageId },
  )
  return rows.map((r) => r.date)
}

export async function insertDailyDownloads(
  qx: QueryExecutor,
  packageId: string,
  days: Array<{ day: string; downloads: number }>,
): Promise<string[]> {
  if (days.length === 0) return []
  const rowCount = await qx.result(
    `INSERT INTO downloads_daily (package_id, date, count)
     SELECT $(packageId)::bigint, d::date, c
       FROM unnest($(dates)::text[], $(counts)::bigint[]) AS u(d, c)
     ON CONFLICT (package_id, date) DO NOTHING`,
    { packageId, dates: days.map((d) => d.day), counts: days.map((d) => d.downloads) },
  )
  return rowCount > 0 ? ['downloads_daily.date', 'downloads_daily.count'] : []
}
