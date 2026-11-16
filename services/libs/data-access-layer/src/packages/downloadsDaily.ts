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

export async function getNewestDownloadDate(
  qx: QueryExecutor,
  packageId: string,
  upTo: string,
): Promise<string | null> {
  const row: { date: string | null } = await qx.selectOne(
    `SELECT MAX(date)::text AS date FROM downloads_daily
      WHERE package_id = $(packageId)::bigint AND date <= $(upTo)::date`,
    { packageId, upTo },
  )
  return row.date
}

export interface DailyBackfillCandidate {
  id: string
  purl: string
  name: string // npm registry name (namespace/name), for the downloads HTTP call
  firstReleaseAt: string | null
}

// `laneIndex`/`laneCount` shard the due set across concurrent lanes by a stable hash
// of the purl, so each lane drains a disjoint slice (laneCount=1 ⇒ no sharding).
// Restricted to is_critical packages — daily downloads are deep per-package history,
// scoped to the critical set (matching the metadata pass). The 30d download passes run
// over all npm packages, since their counts feed the criticality ranking.
// Scoped packages need one request each, so they are due only `scopedMinAgeDays` after
// their last run, while unscoped ones share bulk requests and are due every cutoff.
export async function getNpmPackagesNeedingDailyBackfill(
  qx: QueryExecutor,
  cutoff: string,
  batchSize: number,
  laneIndex: number,
  laneCount: number,
  scopedMinAgeDays: number,
): Promise<DailyBackfillCandidate[]> {
  const rows: Array<{
    id: string
    purl: string
    name: string
    first_release_at: string | null
  }> = await qx.select(
    `SELECT p.id::text AS id, p.purl AS purl,
            CASE WHEN p.namespace IS NOT NULL THEN p.namespace || '/' || p.name ELSE p.name END AS name,
            p.first_release_at::text AS first_release_at
       FROM packages p
       LEFT JOIN npm_package_state s ON s.purl = p.purl
      WHERE p.ecosystem = 'npm'
        AND p.is_critical = TRUE
        AND (((hashtext(p.purl) % $(laneCount)) + $(laneCount)) % $(laneCount)) = $(laneIndex)
        AND (s.daily_downloads_last_processed_at IS NULL
             OR s.daily_downloads_last_processed_at < $(cutoff)::timestamptz - make_interval(
                  days => CASE WHEN p.namespace IS NOT NULL THEN $(scopedMinAgeDays)::int ELSE 0 END))
      ORDER BY s.daily_downloads_last_processed_at ASC NULLS FIRST, p.purl
      LIMIT $(batchSize)`,
    { cutoff, batchSize, laneIndex, laneCount, scopedMinAgeDays },
  )
  return rows.map((r) => ({
    id: r.id,
    purl: r.purl,
    name: r.name,
    firstReleaseAt: r.first_release_at,
  }))
}

// Structured outcome of a daily-downloads run, stored as JSONB in
// npm_package_state.daily_downloads_run_result.
export interface DailyDownloadsRunResult {
  status: 'success' | 'error'
  httpStatus?: number
  errorKind?: string
  message?: string
}

// Bump the daily-downloads watermark for a package (keyed by purl) and record the run
// outcome. Called after a package's windows are processed — even when npm returned no
// data or the package was skipped on a client error — so it isn't re-selected until
// the next cycle.
export async function markDailyDownloadsProcessed(
  qx: QueryExecutor,
  purl: string,
  result: DailyDownloadsRunResult,
): Promise<void> {
  await qx.result(
    `INSERT INTO npm_package_state (purl, daily_downloads_last_processed_at, daily_downloads_run_result)
     VALUES ($(purl), NOW(), $(result)::jsonb)
     ON CONFLICT (purl) DO UPDATE SET
       daily_downloads_last_processed_at = NOW(),
       daily_downloads_run_result        = EXCLUDED.daily_downloads_run_result`,
    { purl, result: JSON.stringify(result) },
  )
}

export async function insertDailyDownloads(
  qx: QueryExecutor,
  packageId: string,
  days: Array<{ day: string; downloads: number }>,
): Promise<string[]> {
  if (days.length === 0) return []
  const rowCount = await qx.result(
    `INSERT INTO downloads_daily (package_id, date, count, created_at, updated_at)
     SELECT $(packageId)::bigint, d::date, c, NOW(), NOW()
       FROM unnest($(dates)::text[], $(counts)::bigint[]) AS u(d, c)
     ON CONFLICT (package_id, date) DO NOTHING`,
    { packageId, dates: days.map((d) => d.day), counts: days.map((d) => d.downloads) },
  )
  return rowCount > 0 ? ['downloads_daily.date', 'downloads_daily.count'] : []
}

// Unlike insertDailyDownloads, a differing count replaces the stored one, so a day stored before
// npm published it gets corrected. A zero never replaces a stored count.
export async function upsertDailyDownloads(
  qx: QueryExecutor,
  packageId: string,
  days: Array<{ day: string; downloads: number }>,
): Promise<{ inserted: number; overwritten: number }> {
  if (days.length === 0) return { inserted: 0, overwritten: 0 }
  const dates = days.map((d) => d.day).sort()
  // downloads_daily is partitioned, so RETURNING cannot read xmax; the statement snapshot taken
  // before the insert tells new days from overwritten ones. The date bounds prune partitions.
  const row: { inserted: number; overwritten: number } = await qx.selectOne(
    `WITH input AS (
       SELECT d::date AS date, c AS count
         FROM unnest($(dates)::text[], $(counts)::bigint[]) AS u(d, c)
     ),
     existing AS (
       SELECT date FROM downloads_daily
        WHERE package_id = $(packageId)::bigint
          AND date BETWEEN $(minDate)::date AND $(maxDate)::date
          AND date IN (SELECT date FROM input)
     ),
     written AS (
       INSERT INTO downloads_daily (package_id, date, count, created_at, updated_at)
       SELECT $(packageId)::bigint, date, count, NOW(), NOW() FROM input
       ON CONFLICT (package_id, date) DO UPDATE SET
         count = EXCLUDED.count,
         updated_at = NOW()
       WHERE downloads_daily.count IS DISTINCT FROM EXCLUDED.count AND EXCLUDED.count > 0
       RETURNING date
     )
     SELECT COUNT(*) FILTER (WHERE e.date IS NULL)::int AS inserted,
            COUNT(*) FILTER (WHERE e.date IS NOT NULL)::int AS overwritten
       FROM written w
       LEFT JOIN existing e ON e.date = w.date`,
    {
      packageId,
      dates: days.map((d) => d.day),
      counts: days.map((d) => d.downloads),
      minDate: dates[0],
      maxDate: dates[dates.length - 1],
    },
  )
  return { inserted: row.inserted, overwritten: row.overwritten }
}
