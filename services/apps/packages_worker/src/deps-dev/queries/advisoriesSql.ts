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

// No purl here — package_id is resolved in Postgres against the already-ingested `packages`
// table (see ADVISORY_PACKAGES_MERGE_SQL in workflows/ingestAdvisories.ts). Pulling purl from
// BigQuery required scanning all of PackageVersionsLatest (~1.5 TB) just for a join key we
// already have locally (CM-1362).
export function buildAdvisoryPackagesSql(systems: string): string {
  return `
SELECT
  a.SourceID             AS osv_id,
  LOWER(pkg.System)      AS ecosystem,
  pkg.Name               AS package_name,
  pkg.AffectedVersions   AS range_raw,
  pkg.UnaffectedVersions AS unaffected_raw
FROM \`bigquery-public-data.deps_dev_v1.AdvisoriesLatest\` a,
UNNEST(a.Packages) AS pkg
WHERE pkg.System IN (${systems})
  AND pkg.Name NOT LIKE '%>%'
`
}
