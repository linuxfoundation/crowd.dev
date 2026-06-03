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

export interface Last30dCandidate {
  purl: string
  firstReleaseAt: string | null
  isNew: boolean // no watermark row yet -> needs the full window history
}

// `laneIndex`/`laneCount` shard the due set across concurrent lanes by a stable hash
// of the purl, so each lane drains a disjoint slice (laneCount=1 ⇒ no sharding).
export async function getNpmUniversePurlsDueForLast30d(
  qx: QueryExecutor,
  cutoff: string,
  batchSize: number,
  laneIndex: number,
  laneCount: number,
): Promise<Last30dCandidate[]> {
  const rows: Array<{ purl: string; first_release_at: string | null; is_new: boolean }> =
    await qx.select(
      `WITH due AS (
         SELECT pu.purl AS purl, (s.purl IS NULL) AS is_new
           FROM packages_universe pu
           LEFT JOIN npm_package_universe_state s ON s.purl = pu.purl
          WHERE pu.ecosystem = 'npm'
            AND (((hashtext(pu.purl) % $(laneCount)) + $(laneCount)) % $(laneCount)) = $(laneIndex)
            AND (s.downloads_30d_last_run_at IS NULL
                 OR s.downloads_30d_last_run_at < $(cutoff)::timestamptz)
          ORDER BY s.downloads_30d_last_run_at ASC NULLS FIRST, pu.purl
          LIMIT $(batchSize)
       )
       SELECT d.purl AS purl, p.first_release_at::text AS first_release_at, d.is_new
         FROM due d
         LEFT JOIN packages p ON p.purl = d.purl`,
      { cutoff, batchSize, laneIndex, laneCount },
    )
  return rows.map((r) => ({
    purl: r.purl,
    firstReleaseAt: r.first_release_at,
    isNew: r.is_new,
  }))
}

// Bump the last-30d watermark for a package (keyed by purl). Called after a
// package's windows are processed so it isn't re-selected until the next cycle.
export async function markLast30dProcessed(qx: QueryExecutor, purl: string): Promise<void> {
  await qx.result(
    `INSERT INTO npm_package_universe_state (purl, downloads_30d_last_run_at)
     VALUES ($(purl), NOW())
     ON CONFLICT (purl) DO UPDATE SET downloads_30d_last_run_at = NOW()`,
    { purl },
  )
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
