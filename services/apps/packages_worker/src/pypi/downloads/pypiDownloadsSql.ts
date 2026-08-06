export const PYPI_DOWNLOADS_30D_KIND = 'pypi_downloads_30d'
export const PYPI_DOWNLOADS_DAILY_KIND = 'pypi_downloads_daily'

// Staging tables the BQ export lands in before the merge into the final download tables.
export const PYPI_DOWNLOADS_30D_STAGING = 'staging.pypi_downloads_30d_raw'
export const PYPI_DOWNLOADS_DAILY_STAGING = 'staging.pypi_downloads_daily_raw'

const BQ_TABLE = '`bigquery-public-data.pypi.file_downloads`'
// PEP 503 canonical project, BigQuery dialect (REGEXP_REPLACE is global by default).
const BQ_PROJECT_NORM = "REGEXP_REPLACE(LOWER(file.project), r'[-_.]+', '-')"
// Keep NULL installers; only drop known mirror traffic.
const MIRROR_EXCLUDE = "COALESCE(details.installer.name, '') <> 'bandersnatch'"
// PEP 503 canonical name, Postgres dialect — the 'g' flag is required so EVERY separator
// run collapses (Postgres REGEXP_REPLACE otherwise replaces only the first match).
const PG_NAME_NORM = "REGEXP_REPLACE(LOWER(p.name), '[-_.]+', '-', 'g')"

// Aggregate query: net downloads per package over [startDate, endDate], one total each.
export function buildPypiDownloads30dSql({
  startDate,
  endDate,
}: {
  startDate: string
  endDate: string
}): string {
  return `
SELECT
  ${BQ_PROJECT_NORM} AS project,
  COUNT(*) AS downloads
FROM ${BQ_TABLE}
WHERE DATE(timestamp) BETWEEN DATE('${startDate}') AND DATE('${endDate}')
  AND ${MIRROR_EXCLUDE}
GROUP BY project
`
}

// Aggregate query: per-day downloads per package over [startDate, endDate]. Like every other BQ
// job here (see deps.dev), it exports all projects and lets the Postgres merge scope to our
// packages (is_critical) — we never push our package list into BigQuery.
export function buildPypiDownloadsDailySql({
  startDate,
  endDate,
}: {
  startDate: string
  endDate: string
}): string {
  return `
SELECT
  ${BQ_PROJECT_NORM} AS project,
  DATE(timestamp) AS day,
  COUNT(*) AS downloads
FROM ${BQ_TABLE}
WHERE DATE(timestamp) BETWEEN DATE('${startDate}') AND DATE('${endDate}')
  AND ${MIRROR_EXCLUDE}
GROUP BY project, day
`
}

// Merge statements for the 30d window: insert into downloads_last_30d, and (only for the
// latest window) mirror the count onto packages.downloads_last_30d.
export function buildPypiDownloads30dMergeSql({
  startDate,
  endDate,
  mirrorToPackages,
}: {
  startDate: string
  endDate: string
  mirrorToPackages: boolean
}): string[] {
  const insert = `
INSERT INTO downloads_last_30d (purl, start_date, end_date, count, created_at, updated_at)
SELECT p.purl, DATE '${startDate}', DATE '${endDate}', s.downloads, NOW(), NOW()
FROM ${PYPI_DOWNLOADS_30D_STAGING} s
JOIN packages p ON p.ecosystem = 'pypi'
  AND ${PG_NAME_NORM} = s.project
ON CONFLICT (purl, end_date) DO UPDATE SET
  count = EXCLUDED.count,
  start_date = EXCLUDED.start_date,
  updated_at = NOW()
`
  if (!mirrorToPackages) return [insert]

  const mirror = `
UPDATE packages p
SET downloads_last_30d = s.downloads
FROM ${PYPI_DOWNLOADS_30D_STAGING} s
WHERE p.ecosystem = 'pypi'
  AND ${PG_NAME_NORM} = s.project
  AND p.downloads_last_30d IS DISTINCT FROM s.downloads
`
  return [insert, mirror]
}

// Merge statement for daily downloads into downloads_daily, scoped to critical pypi packages.
export function buildPypiDownloadsDailyMergeSql(): string {
  return `
INSERT INTO downloads_daily (package_id, date, count, created_at, updated_at)
SELECT p.id, s.day, s.downloads, NOW(), NOW()
FROM ${PYPI_DOWNLOADS_DAILY_STAGING} s
JOIN packages p ON p.ecosystem = 'pypi' AND p.is_critical
  AND ${PG_NAME_NORM} = s.project
ON CONFLICT (package_id, date) DO UPDATE SET
  count = EXCLUDED.count,
  updated_at = NOW()
`
}
