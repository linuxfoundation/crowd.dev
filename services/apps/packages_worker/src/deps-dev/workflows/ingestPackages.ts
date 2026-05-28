import { proxyActivities } from '@temporalio/workflow'

import type * as depsDevActivities from '../activities'

const { bqExportToGcs } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '1 hour',
  retry: { maximumAttempts: 3, initialInterval: '1 minute', backoffCoefficient: 2 },
})

const { gcsParquetToStaging } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '2 hours',
  retry: { maximumAttempts: 2 },
})

const { mergeStagingToTable } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '1 hour',
  retry: { maximumAttempts: 1 },
})

const SYSTEMS = `'NPM', 'GO', 'MAVEN', 'PYPI', 'NUGET', 'CARGO'`

const PACKAGES_SQL_FULL = `
SELECT
  LOWER(System)  AS ecosystem,
  Name           AS raw_name,
  Purl           AS purl,
  Description    AS description,
  Licenses       AS licenses,
  Version        AS latest_version,
  (SELECT URL FROM UNNEST(Links) WHERE Label='SOURCE_REPO' ORDER BY URL LIMIT 1) AS declared_repo_url,
  (SELECT URL FROM UNNEST(Links) WHERE Label='HOMEPAGE'    ORDER BY URL LIMIT 1) AS homepage,
  MIN(UpstreamPublishedAt) OVER (PARTITION BY System, Name) AS first_release_at,
  MAX(UpstreamPublishedAt) OVER (PARTITION BY System, Name) AS latest_release_at,
  COUNT(*)                 OVER (PARTITION BY System, Name) AS versions_count
FROM \`bigquery-public-data.deps_dev_v1.PackageVersionsLatest\`
WHERE System IN (${SYSTEMS})
  AND Purl IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY System, Name ORDER BY UpstreamPublishedAt DESC) = 1
`

function buildIncrementalSql(today: string, watermark: string): string {
  return `
WITH today AS (
  SELECT
    LOWER(System)  AS ecosystem,
    Name           AS raw_name,
    Purl           AS purl,
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
  WHERE SnapshotAt = TIMESTAMP('${today}')
    AND System IN (${SYSTEMS})
    AND Purl IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY System, Name ORDER BY UpstreamPublishedAt DESC) = 1
),
last_watermark AS (
  SELECT DISTINCT System, Name, ANY_VALUE(Purl) AS Purl, ANY_VALUE(Version) AS Version,
         MAX(UpstreamPublishedAt) AS UpstreamPublishedAt
  FROM \`bigquery-public-data.deps_dev_v1.PackageVersions\`
  WHERE SnapshotAt = TIMESTAMP('${watermark}')
    AND System IN (${SYSTEMS})
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

const STAGING_TABLE = 'staging.osspckgs_packages_raw'

const STAGING_DDL = `
CREATE UNLOGGED TABLE IF NOT EXISTS staging.osspckgs_packages_raw (
  ecosystem        text,
  raw_name         text,
  purl             text,
  description      text,
  licenses         text[],
  latest_version   text,
  declared_repo_url text,
  homepage         text,
  first_release_at timestamptz,
  latest_release_at timestamptz,
  versions_count   int
)
`

const MERGE_SQL = `
INSERT INTO packages (
  ecosystem, namespace, name, purl, description, licenses,
  latest_version, declared_repository_url, homepage,
  first_release_at, latest_release_at, versions_count,
  ingestion_source, last_synced_at
)
SELECT
  s.ecosystem,
  CASE
    WHEN s.ecosystem = 'maven' THEN SPLIT_PART(s.raw_name, ':', 1)
    WHEN s.raw_name LIKE '@%/%'  THEN SPLIT_PART(s.raw_name, '/', 1)
    ELSE NULL
  END,
  CASE
    WHEN s.ecosystem = 'maven' THEN SPLIT_PART(s.raw_name, ':', 2)
    WHEN s.raw_name LIKE '@%/%'  THEN SPLIT_PART(s.raw_name, '/', 2)
    ELSE s.raw_name
  END,
  s.purl, s.description, s.licenses,
  s.latest_version, s.declared_repo_url, s.homepage,
  s.first_release_at, s.latest_release_at, s.versions_count,
  'deps_dev', NOW()
FROM staging.osspckgs_packages_raw s
ON CONFLICT (purl) DO UPDATE SET
  description             = EXCLUDED.description,
  licenses                = EXCLUDED.licenses,
  latest_version          = EXCLUDED.latest_version,
  declared_repository_url = EXCLUDED.declared_repository_url,
  homepage                = EXCLUDED.homepage,
  first_release_at        = EXCLUDED.first_release_at,
  latest_release_at       = EXCLUDED.latest_release_at,
  versions_count          = EXCLUDED.versions_count,
  ingestion_source        = EXCLUDED.ingestion_source,
  last_synced_at          = NOW()
`

const PG_COLUMNS = [
  'ecosystem', 'raw_name', 'purl', 'description', 'licenses',
  'latest_version', 'declared_repo_url', 'homepage',
  'first_release_at', 'latest_release_at', 'versions_count',
]

export async function ingestPackages(opts: {
  runId: string
  syncMode: 'full' | 'incremental'
  today: string
  watermark: string | null
}): Promise<void> {
  const sql =
    opts.syncMode === 'full'
      ? PACKAGES_SQL_FULL
      : buildIncrementalSql(opts.today, opts.watermark!)

  const exportResult = await bqExportToGcs({
    jobKind: 'packages',
    sql,
    runId: opts.runId,
    syncMode: opts.syncMode,
    snapshotAt: opts.today,
    maxBytesGb: 400,
  })

  await gcsParquetToStaging({
    jobId: exportResult.jobId,
    gcsPrefix: exportResult.gcsPrefix,
    stagingTable: STAGING_TABLE,
    stagingDdl: STAGING_DDL,
    pgColumns: PG_COLUMNS,
  })

  await mergeStagingToTable({
    jobId: exportResult.jobId,
    mergeSql: MERGE_SQL,
  })
}
