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
}

// BREADTH selection. A purl is "due for the latest window" while its breadth watermark
// (`downloads_30d_last_run_at`) is older than this run's cutoff (or absent). The watermark
// is bumped once its current 30-day window is refreshed, so the monthly run touches every
// package's latest window exactly once and the denormalized number lands across all npm
// packages before any deep history is filled. Older history is a separate pass keyed on
// `downloads_30d_history_backfilled_at` — see getNpmPurlsDueForLast30dHistory.
export async function getNpmPurlsDueForLatest30d(
  qx: QueryExecutor,
  cutoff: string,
  batchSize: number,
  laneIndex: number,
  laneCount: number,
): Promise<Last30dCandidate[]> {
  const rows: Array<{ purl: string; first_release_at: string | null }> = await qx.select(
    `WITH due AS (
         SELECT p.purl AS purl, p.first_release_at
           FROM packages p
           LEFT JOIN npm_package_state s ON s.purl = p.purl
          WHERE p.ecosystem = 'npm'
            AND (((hashtext(p.purl) % $(laneCount)) + $(laneCount)) % $(laneCount)) = $(laneIndex)
            AND (s.downloads_30d_last_run_at IS NULL
                 OR s.downloads_30d_last_run_at < $(cutoff)::timestamptz)
          ORDER BY s.downloads_30d_last_run_at ASC NULLS FIRST, p.purl
          LIMIT $(batchSize)
       )
       SELECT purl, first_release_at::text AS first_release_at FROM due`,
    { cutoff, batchSize, laneIndex, laneCount },
  )
  return rows.map((r) => ({
    purl: r.purl,
    firstReleaseAt: r.first_release_at,
  }))
}

// DEPTH selection. A purl needs history backfill once its latest window is present
// (`downloads_30d_last_run_at IS NOT NULL` — breadth ran) but its full older history has
// not yet been filled (`downloads_30d_history_backfilled_at IS NULL`). This keeps the work
// strictly breadth-first per package: history is never fetched before the latest window.
// Sharded the same way; oldest-breadth-first so the longest-waiting packages drain first.
export async function getNpmPurlsDueForLast30dHistory(
  qx: QueryExecutor,
  batchSize: number,
  laneIndex: number,
  laneCount: number,
): Promise<Last30dCandidate[]> {
  const rows: Array<{ purl: string; first_release_at: string | null }> = await qx.select(
    `WITH due AS (
         SELECT p.purl AS purl, p.first_release_at, s.downloads_30d_last_run_at AS last_run_at
           FROM packages p
           JOIN npm_package_state s ON s.purl = p.purl
          WHERE p.ecosystem = 'npm'
            AND (((hashtext(p.purl) % $(laneCount)) + $(laneCount)) % $(laneCount)) = $(laneIndex)
            AND s.downloads_30d_last_run_at IS NOT NULL
            AND s.downloads_30d_history_backfilled_at IS NULL
          ORDER BY s.downloads_30d_last_run_at ASC, p.purl
          LIMIT $(batchSize)
       )
       SELECT purl, first_release_at::text AS first_release_at FROM due`,
    { batchSize, laneIndex, laneCount },
  )
  return rows.map((r) => ({
    purl: r.purl,
    firstReleaseAt: r.first_release_at,
  }))
}

// Structured outcome of a last-30d run, stored as JSONB in
// npm_package_state.downloads_30d_run_result.
export interface Last30dRunResult {
  status: 'success' | 'error'
  httpStatus?: number
  errorKind?: string
  message?: string
}

// Bump the BREADTH watermark (`downloads_30d_last_run_at`) for a package and record the
// run outcome. Called by the latest-window refresh once a package's current 30-day window
// is processed — even when skipped on a client error — so it isn't re-selected this cycle.
export async function markLast30dProcessed(
  qx: QueryExecutor,
  purl: string,
  result: Last30dRunResult,
): Promise<void> {
  await qx.result(
    `INSERT INTO npm_package_state (purl, downloads_30d_last_run_at, downloads_30d_run_result)
     VALUES ($(purl), NOW(), $(result)::jsonb)
     ON CONFLICT (purl) DO UPDATE SET
       downloads_30d_last_run_at = NOW(),
       downloads_30d_run_result  = EXCLUDED.downloads_30d_run_result`,
    { purl, result: JSON.stringify(result) },
  )
}

// Bump the DEPTH watermark (`downloads_30d_history_backfilled_at`) for a package: its full
// older history is now present in downloads_last_30d (or it was given up on via a client
// error). The row already exists — breadth inserted it — so this is a plain UPDATE; a purl
// is only history-backfilled after its latest window ran, so it is never created here.
export async function markLast30dHistoryBackfilled(
  qx: QueryExecutor,
  purl: string,
  result: Last30dRunResult,
): Promise<void> {
  await qx.result(
    `UPDATE npm_package_state
        SET downloads_30d_history_backfilled_at = NOW(),
            downloads_30d_run_result            = $(result)::jsonb
      WHERE purl = $(purl)`,
    { purl, result: JSON.stringify(result) },
  )
}

export async function upsertLast30dDownload(
  qx: QueryExecutor,
  purl: string,
  startDate: string,
  endDate: string,
  count: number,
  mirrorToPackages: boolean,
): Promise<string[]> {
  const row: { changed_fields: string[] } = await qx.selectOne(
    `WITH old AS (
       SELECT start_date, count FROM downloads_last_30d WHERE purl = $(purl) AND end_date = $(endDate)::date
     ),
     ins AS (
       INSERT INTO downloads_last_30d (purl, start_date, end_date, count, created_at, updated_at)
       VALUES ($(purl), $(startDate)::date, $(endDate)::date, $(count), NOW(), NOW())
       ON CONFLICT (purl, end_date) DO UPDATE SET
         count      = EXCLUDED.count,
         start_date = EXCLUDED.start_date,
         updated_at = EXCLUDED.updated_at
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
  if (mirrorToPackages) {
    // last_synced_at is the Tinybird ENGINE_VER for the packages datasource — it must
    // move whenever a real column changes, or Tinybird can keep serving a stale row.
    const rowCount = await qx.result(
      `UPDATE packages
          SET downloads_last_30d = $(count),
              last_synced_at     = NOW()
        WHERE purl = $(purl) AND downloads_last_30d IS DISTINCT FROM $(count)`,
      { count, purl },
    )
    if (rowCount > 0) changed.push('packages.downloads_last_30d')
  }
  return changed
}
