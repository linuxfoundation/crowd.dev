import { proxyActivities } from '@temporalio/workflow'

import type * as depsDevActivities from '../activities'

const { bqExportToGcs } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '1 hour',
  retry: { maximumAttempts: 3, initialInterval: '1 minute', backoffCoefficient: 2 },
})

const { gcsParquetToStaging } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '1 hour',
  retry: { maximumAttempts: 2 },
})

const { mergeStagingToTable } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '30 minutes',
  retry: { maximumAttempts: 1 },
})

// Both full and incremental use AdvisoriesLatest — no partitioned history table exists.
// ON CONFLICT (osv_id) DO UPDATE handles upserts idempotently.
const ADVISORIES_SQL = `
SELECT
  SourceID                      AS osv_id,
  Source                        AS source,
  SourceURL                     AS source_url,
  Title                         AS summary,
  Description                   AS details,
  CVSS3Score                    AS cvss,
  NULLIF(Severity, 'UNKNOWN')                AS severity,
  Aliases                       AS aliases,
  Disclosed                     AS published_at
FROM \`bigquery-public-data.deps_dev_v1.AdvisoriesLatest\`
`

const ADVISORY_PACKAGES_SQL = `
WITH purl_map AS (
  SELECT System, Name, ANY_VALUE(Purl) AS purl
  FROM \`bigquery-public-data.deps_dev_v1.PackageVersionsLatest\`
  WHERE System IN ('NPM', 'GO', 'MAVEN', 'PYPI', 'NUGET', 'CARGO')
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
WHERE pkg.System IN ('NPM', 'GO', 'MAVEN', 'PYPI', 'NUGET', 'CARGO')
`

const ADVISORIES_STAGING_TABLE = 'staging.osspckgs_advisories_raw'
const ADVISORY_PACKAGES_STAGING_TABLE = 'staging.osspckgs_advisory_packages_raw'

const ADVISORIES_STAGING_DDL = `
CREATE UNLOGGED TABLE IF NOT EXISTS staging.osspckgs_advisories_raw (
  osv_id       text,
  source       text,
  source_url   text,
  summary      text,
  details      text,
  cvss         numeric(3,1),
  severity     text,
  aliases      text[],
  published_at timestamptz
)
`

const ADVISORY_PACKAGES_STAGING_DDL = `
CREATE UNLOGGED TABLE IF NOT EXISTS staging.osspckgs_advisory_packages_raw (
  osv_id         text,
  ecosystem      text,
  package_name   text,
  purl           text,
  range_raw      text,
  unaffected_raw text
)
`

const ADVISORIES_MERGE_SQL = `
INSERT INTO advisories (osv_id, source, source_url, summary, details, cvss, severity, aliases, published_at)
SELECT osv_id, source, source_url, summary, details, cvss, severity, aliases, published_at
FROM staging.osspckgs_advisories_raw
ON CONFLICT (osv_id) DO UPDATE SET
  source       = EXCLUDED.source,
  source_url   = EXCLUDED.source_url,
  summary      = EXCLUDED.summary,
  details      = EXCLUDED.details,
  cvss         = EXCLUDED.cvss,
  severity     = EXCLUDED.severity,
  aliases      = EXCLUDED.aliases,
  published_at = EXCLUDED.published_at
`

const ADVISORY_PACKAGES_MERGE_SQL = `
INSERT INTO advisory_packages (advisory_id, package_id, ecosystem, package_name)
SELECT
  adv.id,
  p.id,
  s.ecosystem,
  s.package_name
FROM staging.osspckgs_advisory_packages_raw s
JOIN advisories adv ON adv.osv_id = s.osv_id
LEFT JOIN packages p ON p.purl = s.purl
ON CONFLICT (advisory_id, ecosystem, package_name) DO UPDATE SET
  package_id = EXCLUDED.package_id
WHERE advisory_packages.package_id IS NULL AND EXCLUDED.package_id IS NOT NULL
`

// Separate statement — must execute after ADVISORY_PACKAGES_MERGE_SQL so advisory_packages rows exist
const ADVISORY_AFFECTED_RANGES_MERGE_SQL = `
INSERT INTO advisory_affected_ranges (advisory_package_id, range_raw, unaffected_raw, introduced_version)
SELECT
  ap.id,
  s.range_raw,
  s.unaffected_raw,
  NULL
FROM staging.osspckgs_advisory_packages_raw s
JOIN advisories adv ON adv.osv_id = s.osv_id
JOIN advisory_packages ap ON ap.advisory_id = adv.id
                          AND ap.ecosystem = s.ecosystem
                          AND ap.package_name = s.package_name
ON CONFLICT (advisory_package_id, COALESCE(introduced_version, ''), COALESCE(fixed_version, '')) DO UPDATE SET
  range_raw      = EXCLUDED.range_raw,
  unaffected_raw = EXCLUDED.unaffected_raw
`

const ADVISORIES_PG_COLUMNS = [
  'osv_id', 'source', 'source_url', 'summary', 'details',
  'cvss', 'severity', 'aliases', 'published_at',
]

const ADVISORY_PACKAGES_PG_COLUMNS = [
  'osv_id', 'ecosystem', 'package_name', 'purl', 'range_raw', 'unaffected_raw',
]

export async function ingestAdvisories(opts: {
  runId: string
  syncMode: 'full' | 'incremental'
  today: string
  watermark: string | null
}): Promise<void> {
  // Step 1: advisories header rows
  const advisoriesExport = await bqExportToGcs({
    jobKind: 'advisories',
    sql: ADVISORIES_SQL,
    runId: opts.runId,
    syncMode: opts.syncMode,
    snapshotAt: opts.today,
    maxBytesGb: 10,
  })

  await gcsParquetToStaging({
    jobId: advisoriesExport.jobId,
    gcsPrefix: advisoriesExport.gcsPrefix,
    stagingTable: ADVISORIES_STAGING_TABLE,
    stagingDdl: ADVISORIES_STAGING_DDL,
    pgColumns: ADVISORIES_PG_COLUMNS,
  })

  await mergeStagingToTable({
    jobId: advisoriesExport.jobId,
    mergeSql: ADVISORIES_MERGE_SQL,
  })

  // Step 2: advisory_packages + affected ranges (FK → advisories must exist first)
  const pkgsExport = await bqExportToGcs({
    jobKind: 'advisory_packages',
    sql: ADVISORY_PACKAGES_SQL,
    runId: opts.runId,
    syncMode: opts.syncMode,
    snapshotAt: opts.today,
    maxBytesGb: 10,
  })

  await gcsParquetToStaging({
    jobId: pkgsExport.jobId,
    gcsPrefix: pkgsExport.gcsPrefix,
    stagingTable: ADVISORY_PACKAGES_STAGING_TABLE,
    stagingDdl: ADVISORY_PACKAGES_STAGING_DDL,
    pgColumns: ADVISORY_PACKAGES_PG_COLUMNS,
  })

  await mergeStagingToTable({
    jobId: pkgsExport.jobId,
    mergeSql: [ADVISORY_PACKAGES_MERGE_SQL, ADVISORY_AFFECTED_RANGES_MERGE_SQL],
  })
}
