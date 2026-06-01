// Both full and incremental use AdvisoriesLatest — no partitioned history table exists.
export const ADVISORIES_SQL = `
SELECT
  SourceID                      AS osv_id,
  Source                        AS source,
  SourceURL                     AS source_url,
  Title                         AS summary,
  Description                   AS details,
  CVSS3Score                    AS cvss,
  NULLIF(Severity, 'UNKNOWN')   AS severity,
  Aliases                       AS aliases,
  Disclosed                     AS published_at
FROM \`bigquery-public-data.deps_dev_v1.AdvisoriesLatest\`
`

export function buildAdvisoryPackagesSql(systems: string): string {
  return `
WITH purl_map AS (
  SELECT System, Name, ANY_VALUE(Purl) AS purl
  FROM \`bigquery-public-data.deps_dev_v1.PackageVersionsLatest\`
  WHERE System IN (${systems})
    AND Purl IS NOT NULL
  GROUP BY System, Name
)
SELECT
  a.SourceID             AS osv_id,
  LOWER(pkg.System)      AS ecosystem,
  pkg.Name               AS package_name,
  pm.purl                AS purl,
  pkg.AffectedVersions   AS range_raw,
  pkg.UnaffectedVersions AS unaffected_raw
FROM \`bigquery-public-data.deps_dev_v1.AdvisoriesLatest\` a,
UNNEST(a.Packages) AS pkg
LEFT JOIN purl_map pm ON pm.System = pkg.System AND pm.Name = pkg.Name
WHERE pkg.System IN (${systems})
`
}
