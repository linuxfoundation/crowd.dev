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
): Promise<string[]> {
  const row: { changed_fields: string[] } = await qx.selectOne(
    `WITH old AS (
       SELECT start_date, count FROM downloads_last_30d WHERE purl = $(purl) AND end_date = $(endDate)::date
     ),
     ins AS (
       INSERT INTO downloads_last_30d (purl, start_date, end_date, count)
       VALUES ($(purl), $(startDate)::date, $(endDate)::date, $(count))
       ON CONFLICT (purl, end_date) DO UPDATE SET
         count      = EXCLUDED.count,
         start_date = EXCLUDED.start_date
       RETURNING start_date, count
     )
     SELECT array_remove(ARRAY[
       CASE WHEN o.start_date IS DISTINCT FROM ins.start_date THEN 'downloads_last_30d.start_date' END,
       CASE WHEN o.count      IS DISTINCT FROM ins.count      THEN 'downloads_last_30d.count' END
     ], NULL) AS changed_fields
     FROM ins LEFT JOIN old o ON true`,
    { purl, startDate, endDate, count },
  )
  const changed = row.changed_fields
  if (mirrorToUniverse) {
    const rowCount = await qx.result(
      `UPDATE packages_universe
          SET downloads_last_30d = $(count)
        WHERE purl = $(purl) AND downloads_last_30d IS DISTINCT FROM $(count)`,
      { count, purl },
    )
    if (rowCount > 0) changed.push('packages_universe.downloads_last_30d')
  }
  return changed
}
