export function buildVersionsFullSql(systems: string): string {
  return `
SELECT
  LOWER(System) AS ecosystem,
  Name          AS raw_name,
  REGEXP_REPLACE(Purl, r'@[^@]*$', '') AS purl,
  Version       AS number,
  UpstreamPublishedAt                                                                   AS published_at,
  COALESCE(NOT VersionInfo.IsRelease, FALSE)                                            AS is_prerelease,
  ARRAY(SELECT DISTINCT l FROM UNNEST(Licenses) AS l WHERE l IS NOT NULL ORDER BY l)   AS licenses
FROM \`bigquery-public-data.deps_dev_v1.PackageVersionsLatest\`
WHERE System IN (${systems})
  AND Purl IS NOT NULL
  AND Name NOT LIKE '%>%'
`
}

export function buildVersionsIncrementalSql(
  today: string,
  watermark: string,
  systems: string,
): string {
  return `
WITH today AS (
  SELECT
    LOWER(System) AS ecosystem,
    Name          AS raw_name,
    REGEXP_REPLACE(Purl, r'@[^@]*$', '') AS purl,
    Version       AS number,
    UpstreamPublishedAt                                                                   AS published_at,
    COALESCE(NOT VersionInfo.IsRelease, FALSE)                                            AS is_prerelease,
    ARRAY(SELECT DISTINCT l FROM UNNEST(Licenses) AS l WHERE l IS NOT NULL ORDER BY l)   AS licenses
  FROM \`bigquery-public-data.deps_dev_v1.PackageVersions\`
  WHERE SnapshotAt >= TIMESTAMP('${today}')
    AND SnapshotAt <  TIMESTAMP(DATE_ADD(DATE '${today}', INTERVAL 1 DAY))
    AND System IN (${systems})
    AND Purl IS NOT NULL
    AND Name NOT LIKE '%>%'
),
last_watermark AS (
  SELECT System, Name, Version, MAX(UpstreamPublishedAt) AS UpstreamPublishedAt
  FROM \`bigquery-public-data.deps_dev_v1.PackageVersions\`
  WHERE SnapshotAt >= TIMESTAMP('${watermark}')
    AND SnapshotAt <  TIMESTAMP(DATE_ADD(DATE '${watermark}', INTERVAL 1 DAY))
    AND System IN (${systems})
    AND Purl IS NOT NULL
    AND Name NOT LIKE '%>%'
  GROUP BY System, Name, Version
)
SELECT t.*
FROM today t
LEFT JOIN last_watermark l
  ON LOWER(l.System) = t.ecosystem AND l.Name = t.raw_name AND l.Version = t.number
WHERE l.Version IS NULL
   OR t.published_at != l.UpstreamPublishedAt
`
}
