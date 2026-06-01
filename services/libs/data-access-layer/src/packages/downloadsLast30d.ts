import { QueryExecutor } from '../queryExecutor'

export async function getExistingLast30dEndDates(
  qx: QueryExecutor,
  purl: string,
  lower: string,
  upper: string,
): Promise<string[]> {
  const rows: Array<{ end_date: string }> = await qx.select(
    `SELECT end_date::text AS end_date
       FROM downloads_last_30d
      WHERE purl = $(purl)
        AND end_date BETWEEN $(lower)::date AND $(upper)::date
      ORDER BY end_date`,
    { purl, lower, upper },
  )
  return rows.map((r) => r.end_date)
}

export async function upsertLast30dDownload(
  qx: QueryExecutor,
  purl: string,
  startDate: string,
  endDate: string,
  count: number,
  mirrorToUniverse: boolean,
): Promise<void> {
  await qx.result(
    `INSERT INTO downloads_last_30d (purl, start_date, end_date, count)
     VALUES ($(purl), $(startDate)::date, $(endDate)::date, $(count))
     ON CONFLICT (purl, end_date) DO UPDATE SET
       count      = EXCLUDED.count,
       start_date = EXCLUDED.start_date`,
    { purl, startDate, endDate, count },
  )
  if (mirrorToUniverse) {
    await qx.result(
      `UPDATE packages_universe SET downloads_last_30d = $(count) WHERE purl = $(purl)`,
      { count, purl },
    )
  }
}
