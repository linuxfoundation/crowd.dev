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

export interface DailyBackfillCandidate {
  id: string
  purl: string
  name: string
  firstReleaseAt: string | null
}

export async function getTrackedPackagesNeedingDailyBackfill(
  qx: QueryExecutor,
  names: string[],
  purls: string[],
  cutoff: string,
  batchSize: number,
): Promise<DailyBackfillCandidate[]> {
  const rows: Array<{
    id: string
    purl: string
    name: string
    first_release_at: string | null
  }> = await qx.select(
    `WITH wl AS (
       SELECT name, purl FROM unnest($(names)::text[], $(purls)::text[]) AS t(name, purl)
     )
     SELECT p.id::text AS id, p.purl, wl.name, p.first_release_at::text AS first_release_at
       FROM wl
       JOIN packages p ON p.ecosystem = 'npm' AND p.purl = wl.purl
       LEFT JOIN npm_package_state s ON s.name = wl.name
      WHERE s.daily_downloads_last_processed_at IS NULL
         OR s.daily_downloads_last_processed_at < $(cutoff)::timestamptz
      ORDER BY s.daily_downloads_last_processed_at ASC NULLS FIRST, p.purl
      LIMIT $(batchSize)`,
    { names, purls, cutoff, batchSize },
  )
  return rows.map((r) => ({
    id: r.id,
    purl: r.purl,
    name: r.name,
    firstReleaseAt: r.first_release_at,
  }))
}

// Bump the daily-downloads watermark for a package (keyed by npm name). Called
// after a package's windows are processed — even when npm returned no data — so
// it isn't re-selected until the next cycle.
export async function markDailyDownloadsProcessed(qx: QueryExecutor, name: string): Promise<void> {
  await qx.result(
    `INSERT INTO npm_package_state (name, daily_downloads_last_processed_at)
     VALUES ($(name), NOW())
     ON CONFLICT (name) DO UPDATE SET daily_downloads_last_processed_at = NOW()`,
    { name },
  )
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
