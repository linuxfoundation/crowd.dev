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

const VERSIONS_SQL_FULL = `
SELECT
  LOWER(System) AS ecosystem,
  Name          AS raw_name,
  Purl          AS purl,
  Version       AS number,
  UpstreamPublishedAt                                      AS published_at,
  NOT VersionInfo.IsRelease                                AS is_prerelease,
  ARRAY_AGG(DISTINCT l ORDER BY l IGNORE NULLS)            AS licenses
FROM \`bigquery-public-data.deps_dev_v1.PackageVersionsLatest\`
LEFT JOIN UNNEST(Licenses) AS l ON TRUE
WHERE System IN (${SYSTEMS})
  AND Purl IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6
`

function buildIncrementalSql(today: string, watermark: string): string {
  return `
WITH today AS (
  SELECT
    LOWER(System) AS ecosystem,
    Name          AS raw_name,
    Purl          AS purl,
    Version       AS number,
    UpstreamPublishedAt                                    AS published_at,
    COALESCE(NOT VersionInfo.IsRelease, FALSE)             AS is_prerelease,
    ARRAY_AGG(DISTINCT l ORDER BY l IGNORE NULLS)          AS licenses
  FROM \`bigquery-public-data.deps_dev_v1.PackageVersions\`
  LEFT JOIN UNNEST(Licenses) AS l ON TRUE
  WHERE SnapshotAt = TIMESTAMP('${today}')
    AND System IN (${SYSTEMS})
    AND Purl IS NOT NULL
  GROUP BY 1, 2, 3, 4, 5, 6
),
last_watermark AS (
  SELECT System, Name, Version, MAX(UpstreamPublishedAt) AS UpstreamPublishedAt
  FROM \`bigquery-public-data.deps_dev_v1.PackageVersions\`
  WHERE SnapshotAt = TIMESTAMP('${watermark}')
    AND System IN (${SYSTEMS})
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

const STAGING_TABLE = 'staging.osspckgs_versions_raw'

const STAGING_DDL = `
CREATE UNLOGGED TABLE IF NOT EXISTS staging.osspckgs_versions_raw (
  ecosystem     text,
  raw_name      text,
  purl          text,
  number        text,
  published_at  timestamptz,
  is_prerelease bool,
  licenses      text[]
)
`

// TODO(Step 11 optimization): for full loads, drop secondary indexes on versions_p0…versions_p31
// before merge and rebuild after — 5-10× speedup. Requires knowing exact index names from the
// migration. Skip for now; add once confirmed against a live schema.
const MERGE_SQL = `
INSERT INTO versions (
  package_id, ecosystem, number, published_at, is_prerelease, licenses, last_synced_at
)
SELECT
  p.id, s.ecosystem, s.number, s.published_at, s.is_prerelease, s.licenses, NOW()
FROM staging.osspckgs_versions_raw s
JOIN packages p ON p.purl = s.purl
ON CONFLICT (package_id, number) DO UPDATE SET
  published_at   = EXCLUDED.published_at,
  is_prerelease  = EXCLUDED.is_prerelease,
  licenses       = EXCLUDED.licenses,
  last_synced_at = NOW()
`

const PG_COLUMNS = [
  'ecosystem', 'raw_name', 'purl', 'number',
  'published_at', 'is_prerelease', 'licenses',
]

export async function ingestVersions(opts: {
  runId: string
  syncMode: 'full' | 'incremental'
  today: string
  watermark: string | null
}): Promise<void> {
  const sql =
    opts.syncMode === 'full'
      ? VERSIONS_SQL_FULL
      : buildIncrementalSql(opts.today, opts.watermark!)

  const exportResult = await bqExportToGcs({
    jobKind: 'versions',
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
