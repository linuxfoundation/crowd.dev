import { QueryExecutor } from '../queryExecutor'

// Structured outcome of a packagist ingest run, stored as JSONB in
// packagist_package_state.{metadata,downloads_30d,daily_downloads}_run_result.
export interface PackagistRunResult {
  status: 'success' | 'error'
  attempts: number
  httpStatus?: number
  errorKind?: string
  message?: string
}

export interface PackagistMetadataCandidate {
  purl: string
  // Last-Modified from the previous p2 fetch, replayed as If-Modified-Since.
  metadataLastModified: string | null
}

export interface PackagistDailyCandidate {
  purl: string
  packageId: string
}

export async function markPackagistMetadataScanned(
  qx: QueryExecutor,
  purl: string,
  result: PackagistRunResult,
  metadataLastModified?: string | null,
): Promise<void> {
  await qx.result(
    `INSERT INTO packagist_package_state (purl, metadata_run_result, metadata_last_run_at, metadata_last_modified)
     VALUES ($(purl), $(result)::jsonb, NOW(), $(metadataLastModified))
     ON CONFLICT (purl) DO UPDATE SET
       metadata_run_result    = EXCLUDED.metadata_run_result,
       metadata_last_run_at   = EXCLUDED.metadata_last_run_at,
       metadata_last_modified = COALESCE(EXCLUDED.metadata_last_modified, packagist_package_state.metadata_last_modified)`,
    { purl, result: JSON.stringify(result), metadataLastModified: metadataLastModified ?? null },
  )
}

export async function getPackagistMetadataDuePurls(
  qx: QueryExecutor,
  afterPurl: string,
  batchSize: number,
  refreshDays: number,
  onlyCritical: boolean,
): Promise<PackagistMetadataCandidate[]> {
  const rows: Array<{ purl: string; metadata_last_modified: string | null }> = await qx.select(
    `SELECT p.purl, s.metadata_last_modified
       FROM packages p
       LEFT JOIN packagist_package_state s ON s.purl = p.purl
      WHERE p.ecosystem = 'packagist'
        AND (NOT $(onlyCritical) OR p.is_critical = TRUE)
        AND p.purl > $(afterPurl)
        AND (
          s.metadata_last_run_at IS NULL
          OR s.metadata_last_run_at < NOW() - ($(refreshDays) || ' days')::interval
        )
      ORDER BY p.purl
      LIMIT $(batchSize)`,
    { afterPurl, batchSize, refreshDays, onlyCritical },
  )
  return rows.map((r) => ({
    purl: r.purl,
    metadataLastModified: r.metadata_last_modified,
  }))
}

export async function getPackagist30dDuePurls(
  qx: QueryExecutor,
  cutoff: string,
  afterPurl: string,
  batchSize: number,
): Promise<string[]> {
  const rows: Array<{ purl: string }> = await qx.select(
    `SELECT p.purl
       FROM packages p
       LEFT JOIN packagist_package_state s ON s.purl = p.purl
      WHERE p.ecosystem = 'packagist'
        AND p.purl > $(afterPurl)
        AND (
          s.downloads_30d_last_run_at IS NULL
          OR s.downloads_30d_last_run_at < $(cutoff)::timestamptz
        )
      ORDER BY p.purl
      LIMIT $(batchSize)`,
    { cutoff, afterPurl, batchSize },
  )
  return rows.map((r) => r.purl)
}

export async function markPackagist30dProcessed(
  qx: QueryExecutor,
  purl: string,
  result: PackagistRunResult,
): Promise<void> {
  await qx.result(
    `INSERT INTO packagist_package_state (purl, downloads_30d_run_result, downloads_30d_last_run_at)
     VALUES ($(purl), $(result)::jsonb, NOW())
     ON CONFLICT (purl) DO UPDATE SET
       downloads_30d_run_result  = EXCLUDED.downloads_30d_run_result,
       downloads_30d_last_run_at = EXCLUDED.downloads_30d_last_run_at`,
    { purl, result: JSON.stringify(result) },
  )
}

export async function getPackagistDailyDownloadsDue(
  qx: QueryExecutor,
  cutoff: string,
  afterPurl: string,
  batchSize: number,
): Promise<PackagistDailyCandidate[]> {
  const rows: Array<{ purl: string; package_id: string }> = await qx.select(
    `SELECT p.purl, p.id::text AS package_id
       FROM packages p
       LEFT JOIN packagist_package_state s ON s.purl = p.purl
      WHERE p.ecosystem = 'packagist'
        AND p.is_critical = TRUE
        AND p.purl > $(afterPurl)
        AND (
          s.daily_downloads_last_run_at IS NULL
          OR s.daily_downloads_last_run_at < $(cutoff)::timestamptz
        )
      ORDER BY p.purl
      LIMIT $(batchSize)`,
    { cutoff, afterPurl, batchSize },
  )
  return rows.map((r) => ({ purl: r.purl, packageId: r.package_id }))
}

export async function markPackagistDailyProcessed(
  qx: QueryExecutor,
  purl: string,
  result: PackagistRunResult,
): Promise<void> {
  await qx.result(
    `INSERT INTO packagist_package_state (purl, daily_downloads_run_result, daily_downloads_last_run_at)
     VALUES ($(purl), $(result)::jsonb, NOW())
     ON CONFLICT (purl) DO UPDATE SET
       daily_downloads_run_result  = EXCLUDED.daily_downloads_run_result,
       daily_downloads_last_run_at = EXCLUDED.daily_downloads_last_run_at`,
    { purl, result: JSON.stringify(result) },
  )
}
