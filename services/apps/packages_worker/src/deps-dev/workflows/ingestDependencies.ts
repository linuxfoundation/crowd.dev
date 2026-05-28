import { proxyActivities } from '@temporalio/workflow'

import type * as depsDevActivities from '../activities'

const { bqExportToGcs } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '2 hours',
  retry: { maximumAttempts: 3, initialInterval: '1 minute', backoffCoefficient: 2 },
})

const { gcsParquetToStaging } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '4 hours',
  retry: { maximumAttempts: 2 },
})

const { mergeStagingToTable } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '2 hours',
  retry: { maximumAttempts: 1 },
})

const SYSTEMS = `'NPM', 'GO', 'MAVEN', 'PYPI', 'NUGET', 'CARGO'`

const DEPS_SQL_FULL = `
WITH purl_map AS (
  SELECT System, Name, ANY_VALUE(Purl) AS purl
  FROM \`bigquery-public-data.deps_dev_v1.PackageVersionsLatest\`
  WHERE System IN (${SYSTEMS})
    AND Purl IS NOT NULL
  GROUP BY System, Name
)
SELECT
  LOWER(e.System)     AS ecosystem,
  e.Name              AS root_name,
  pm_root.purl        AS root_purl,
  e.Version           AS root_version,
  e.To.Name           AS to_name,
  pm_to.purl          AS to_purl,
  e.To.Version        AS to_version,
  e.Requirement       AS version_constraint
FROM \`bigquery-public-data.deps_dev_v1.DependencyGraphEdgesLatest\` e
LEFT JOIN purl_map pm_root ON pm_root.System = e.System AND pm_root.Name = e.Name
LEFT JOIN purl_map pm_to   ON pm_to.System   = e.System AND pm_to.Name   = e.To.Name
WHERE e.System IN (${SYSTEMS})
  AND e.From.Name = e.Name
  AND e.From.Version = e.Version
  AND pm_root.purl IS NOT NULL
  AND pm_to.purl   IS NOT NULL
`

function buildIncrementalSql(today: string, watermark: string): string {
  return `
WITH purl_map AS (
  SELECT System, Name, ANY_VALUE(Purl) AS purl
  FROM \`bigquery-public-data.deps_dev_v1.PackageVersionsLatest\`
  WHERE System IN (${SYSTEMS})
    AND Purl IS NOT NULL
  GROUP BY System, Name
),
today AS (
  SELECT
    LOWER(e.System)     AS ecosystem,
    e.Name              AS root_name,
    pm_root.purl        AS root_purl,
    e.Version           AS root_version,
    e.To.Name           AS to_name,
    pm_to.purl          AS to_purl,
    e.To.Version        AS to_version,
    e.Requirement       AS version_constraint
  FROM \`bigquery-public-data.deps_dev_v1.DependencyGraphEdges\` e
  LEFT JOIN purl_map pm_root ON pm_root.System = e.System AND pm_root.Name = e.Name
  LEFT JOIN purl_map pm_to   ON pm_to.System   = e.System AND pm_to.Name   = e.To.Name
  WHERE e.SnapshotAt = TIMESTAMP('${today}')
    AND e.System IN (${SYSTEMS})
    AND e.From.Name = e.Name AND e.From.Version = e.Version
    AND pm_root.purl IS NOT NULL
    AND pm_to.purl   IS NOT NULL
),
last_watermark AS (
  SELECT e.System, e.Name, e.Version, e.To.Name AS to_name, e.To.Version AS to_version
  FROM \`bigquery-public-data.deps_dev_v1.DependencyGraphEdges\` e
  WHERE e.SnapshotAt = TIMESTAMP('${watermark}')
    AND e.System IN (${SYSTEMS})
    AND e.From.Name = e.Name AND e.From.Version = e.Version
  GROUP BY e.System, e.Name, e.Version, e.To.Name, e.To.Version
)
SELECT t.*
FROM today t
LEFT JOIN last_watermark l
  ON LOWER(l.System) = t.ecosystem AND l.Name = t.root_name AND l.Version = t.root_version
 AND l.to_name = t.to_name AND l.to_version = t.to_version
WHERE l.to_name IS NULL
`
}

const STAGING_TABLE = 'staging.osspckgs_deps_raw'

const STAGING_DDL = `
CREATE UNLOGGED TABLE IF NOT EXISTS staging.osspckgs_deps_raw (
  ecosystem        text,
  root_name        text,
  root_purl        text,
  root_version     text,
  to_name          text,
  to_purl          text,
  to_version       text,
  version_constraint text
)
`

const MERGE_SQL = `
INSERT INTO package_dependencies (
  package_id, version_id, depends_on_id, depends_on_version_id,
  version_constraint, dependency_kind, is_optional
)
SELECT
  pv.package_id,
  pv.id,
  pd.id,
  dv.id,
  s.version_constraint,
  'direct',
  FALSE
FROM staging.osspckgs_deps_raw s
JOIN packages pp ON pp.purl = s.root_purl
JOIN versions pv ON pv.package_id = pp.id AND pv.number = s.root_version
JOIN packages pd ON pd.purl = s.to_purl
LEFT JOIN versions dv ON dv.package_id = pd.id AND dv.number = s.to_version
ON CONFLICT (version_id, depends_on_id, dependency_kind) DO UPDATE SET
  version_constraint    = EXCLUDED.version_constraint,
  depends_on_version_id = EXCLUDED.depends_on_version_id
`

const PG_COLUMNS = [
  'ecosystem', 'root_name', 'root_purl', 'root_version',
  'to_name', 'to_purl', 'to_version', 'version_constraint',
]

export async function ingestDependencies(opts: {
  runId: string
  syncMode: 'full' | 'incremental'
  today: string
  watermark: string | null
}): Promise<void> {
  const sql =
    opts.syncMode === 'full'
      ? DEPS_SQL_FULL
      : buildIncrementalSql(opts.today, opts.watermark!)

  const exportResult = await bqExportToGcs({
    jobKind: 'package_dependencies',
    sql,
    runId: opts.runId,
    syncMode: opts.syncMode,
    snapshotAt: opts.today,
    maxBytesGb: 1200,
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
