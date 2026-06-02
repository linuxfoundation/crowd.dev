export function buildPackagesFullSql(systems: string): string {
  return `
SELECT
  LOWER(System)  AS ecosystem,
  Name           AS raw_name,
  REGEXP_REPLACE(Purl, r'@[^@]*$', '') AS purl,
  Description    AS description,
  Licenses       AS licenses,
  Version        AS latest_version,
  (SELECT URL FROM UNNEST(Links) WHERE Label='SOURCE_REPO' ORDER BY URL LIMIT 1) AS declared_repo_url,
  (SELECT URL FROM UNNEST(Links) WHERE Label='HOMEPAGE'    ORDER BY URL LIMIT 1) AS homepage,
  MIN(UpstreamPublishedAt) OVER (PARTITION BY System, Name) AS first_release_at,
  MAX(UpstreamPublishedAt) OVER (PARTITION BY System, Name) AS latest_release_at,
  COUNT(*)                 OVER (PARTITION BY System, Name) AS versions_count
FROM \`bigquery-public-data.deps_dev_v1.PackageVersionsLatest\`
WHERE System IN (${systems})
  AND Purl IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY System, Name ORDER BY UpstreamPublishedAt DESC) = 1
`
}

export function buildPackagesIncrementalSql(
  today: string,
  watermark: string,
  systems: string,
): string {
  return `
WITH today AS (
  SELECT
    LOWER(System)  AS ecosystem,
    Name           AS raw_name,
    REGEXP_REPLACE(Purl, r'@[^@]*$', '') AS purl,
    Description    AS description,
    Licenses       AS licenses,
    Version        AS latest_version,
    (SELECT URL FROM UNNEST(Links) WHERE Label='SOURCE_REPO' ORDER BY URL LIMIT 1) AS declared_repo_url,
    (SELECT URL FROM UNNEST(Links) WHERE Label='HOMEPAGE'    ORDER BY URL LIMIT 1) AS homepage,
    UpstreamPublishedAt,
    MIN(UpstreamPublishedAt) OVER (PARTITION BY System, Name) AS first_release_at,
    MAX(UpstreamPublishedAt) OVER (PARTITION BY System, Name) AS latest_release_at,
    COUNT(*)                 OVER (PARTITION BY System, Name) AS versions_count
  FROM \`bigquery-public-data.deps_dev_v1.PackageVersions\`
  WHERE SnapshotAt >= TIMESTAMP('${today}')
    AND SnapshotAt <  TIMESTAMP(DATE_ADD(DATE '${today}', INTERVAL 1 DAY))
    AND System IN (${systems})
    AND Purl IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY System, Name ORDER BY UpstreamPublishedAt DESC) = 1
),
last_watermark AS (
  SELECT DISTINCT System, Name, ANY_VALUE(Purl) AS Purl, ANY_VALUE(Version) AS Version,
         MAX(UpstreamPublishedAt) AS UpstreamPublishedAt
  FROM \`bigquery-public-data.deps_dev_v1.PackageVersions\`
  WHERE SnapshotAt >= TIMESTAMP('${watermark}')
    AND SnapshotAt <  TIMESTAMP(DATE_ADD(DATE '${watermark}', INTERVAL 1 DAY))
    AND System IN (${systems})
  GROUP BY System, Name
)
SELECT t.* EXCEPT(UpstreamPublishedAt)
FROM today t
LEFT JOIN last_watermark l
  ON l.System = UPPER(t.ecosystem) AND l.Name = t.raw_name
WHERE l.Name IS NULL
   OR t.UpstreamPublishedAt != l.UpstreamPublishedAt
`
}
