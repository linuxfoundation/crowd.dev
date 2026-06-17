import { proxyActivities } from '@temporalio/workflow'

import type * as depsDevActivities from '../activities'
import { ADVISORIES_SQL, buildAdvisoryPackagesSql } from '../queries/advisoriesSql'
import { toSystemsFilter } from '../queries/systems'

const { bqExportToGcs } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '1 hour',
  retry: { maximumAttempts: 3, initialInterval: '1 minute', backoffCoefficient: 2 },
})

const { listParquetFiles } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '5 minutes',
  retry: { maximumAttempts: 3 },
})

const { gcsParquetToStaging } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '1 hour',
  heartbeatTimeout: '2 minutes',
  retry: { maximumAttempts: 2 },
})

const { mergeStagingToTable } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '30 minutes',
  retry: { maximumAttempts: 1 },
})

const ADVISORIES_STAGING_TABLE = 'staging.osspckgs_advisories_raw'
const ADVISORY_PACKAGES_STAGING_TABLE = 'staging.osspckgs_advisory_packages_raw'

const ADVISORIES_STAGING_DDL = `
CREATE UNLOGGED TABLE IF NOT EXISTS staging.osspckgs_advisories_raw (
  osv_id       text,
  source       text,
  source_url   text,
  summary      text,
  details      text,
  cvss         float8,
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
INSERT INTO advisories (osv_id, source, source_url, summary, details, cvss, severity, aliases, published_at, created_at, updated_at)
SELECT osv_id, source, source_url, summary, details,
  CASE WHEN cvss IS NULL OR cvss = 'NaN'::float8 OR cvss = 'Infinity'::float8 OR cvss = '-Infinity'::float8
    THEN NULL ELSE cvss::numeric(3,1) END,
  severity, aliases, published_at, NOW(), NOW()
FROM staging.osspckgs_advisories_raw
ON CONFLICT (osv_id) DO NOTHING
`

const ADVISORY_PACKAGES_MERGE_SQL = `
INSERT INTO advisory_packages (advisory_id, package_id, ecosystem, package_name, created_at, updated_at)
SELECT
  adv.id,
  p.id,
  s.ecosystem,
  s.package_name,
  NOW(), NOW()
FROM staging.osspckgs_advisory_packages_raw s
JOIN advisories adv ON adv.osv_id = s.osv_id
LEFT JOIN packages p ON p.purl = s.purl
ON CONFLICT (advisory_id, ecosystem, package_name) DO NOTHING
`

// Separate statement — must execute after ADVISORY_PACKAGES_MERGE_SQL so advisory_packages rows exist
const ADVISORY_AFFECTED_RANGES_MERGE_SQL = `
INSERT INTO advisory_affected_ranges (advisory_package_id, range_raw, unaffected_raw, introduced_version, created_at, updated_at)
SELECT
  ap.id,
  s.range_raw,
  s.unaffected_raw,
  NULL,
  NOW(), NOW()
FROM staging.osspckgs_advisory_packages_raw s
JOIN advisories adv ON adv.osv_id = s.osv_id
JOIN advisory_packages ap ON ap.advisory_id = adv.id
                          AND ap.ecosystem = s.ecosystem
                          AND ap.package_name = s.package_name
ON CONFLICT (advisory_package_id, COALESCE(introduced_version, ''), COALESCE(fixed_version, ''), COALESCE(last_affected, '')) DO NOTHING
`

const ADVISORIES_PG_COLUMNS = [
  'osv_id',
  'source',
  'source_url',
  'summary',
  'details',
  'cvss',
  'severity',
  'aliases',
  'published_at',
]

const ADVISORY_PACKAGES_PG_COLUMNS = [
  'osv_id',
  'ecosystem',
  'package_name',
  'purl',
  'range_raw',
  'unaffected_raw',
]

const ROWS_PER_CHUNK = 1_000_000

export async function ingestAdvisories(opts: {
  runId: string
  syncMode: 'full' | 'incremental'
  today: string
  watermark: string | null
  ecosystems?: string[]
  reuseExports?: boolean
  exportName?: string
}): Promise<void> {
  const systems = toSystemsFilter(opts.ecosystems)

  // Step 1: advisories header rows
  const advisoriesExport = await bqExportToGcs({
    jobKind: 'advisories',
    sql: ADVISORIES_SQL,
    runId: opts.runId,
    syncMode: opts.syncMode,
    snapshotAt: opts.today,
    maxBytesGb: 10,
    reuseExports: opts.reuseExports,
    exportName: opts.exportName,
    ecosystems: opts.ecosystems,
  })

  const { fileNames: advFileNames, rowCounts: advRowCounts } = await listParquetFiles({
    gcsPrefix: advisoriesExport.gcsPrefix,
  })
  const advTotalFiles = advFileNames.length

  if (advTotalFiles === 0) {
    await mergeStagingToTable({
      jobId: advisoriesExport.jobId,
      mergeSql: [],
      tableNames: [],
      isFinal: true,
    })
  } else {
    const advTotalRows = advRowCounts.reduce((a, b) => a + b, 0)
    const advFilesPerChunk =
      advTotalRows > 0
        ? Math.max(1, Math.round((ROWS_PER_CHUNK * advFileNames.length) / advTotalRows))
        : Math.min(advFileNames.length, 2)
    const advTotalChunks = Math.ceil(advFileNames.length / advFilesPerChunk)
    let priorRowsAffected = 0
    let advPriorStagingRows = 0
    const advPriorTableRowCounts: Record<string, number> = {}

    for (let chunkIndex = 0; chunkIndex < advTotalChunks; chunkIndex++) {
      const start = chunkIndex * advFilesPerChunk
      const chunk = advFileNames.slice(start, start + advFilesPerChunk)
      const isFinal = chunkIndex === advTotalChunks - 1

      const { rowsLoaded } = await gcsParquetToStaging({
        jobId: advisoriesExport.jobId,
        stagingTable: ADVISORIES_STAGING_TABLE,
        stagingDdl: ADVISORIES_STAGING_DDL,
        pgColumns: ADVISORIES_PG_COLUMNS,
        timestampColumns: ['published_at'],
        decimalColumns: ['cvss'],
        fileNames: chunk,
        filesOffset: start,
        totalFiles: advTotalFiles,
        priorStagingRows: advPriorStagingRows,
      })
      advPriorStagingRows += rowsLoaded

      const { rowsAffected, tableRowCounts } = await mergeStagingToTable({
        jobId: advisoriesExport.jobId,
        mergeSql: ADVISORIES_MERGE_SQL,
        tableNames: 'advisories',
        isFinal,
        priorRowsAffected,
        priorTableRowCounts: advPriorTableRowCounts,
        chunkInfo: { index: chunkIndex, total: advTotalChunks },
      })

      priorRowsAffected += rowsAffected
      if (!isFinal) {
        for (const [k, v] of Object.entries(tableRowCounts)) {
          advPriorTableRowCounts[k] = (advPriorTableRowCounts[k] ?? 0) + v
        }
      }
    }
  }

  // Step 2: advisory_packages + affected ranges (FK → advisories must exist first)
  const pkgsExport = await bqExportToGcs({
    jobKind: 'advisory_packages',
    sql: buildAdvisoryPackagesSql(systems),
    runId: opts.runId,
    syncMode: opts.syncMode,
    snapshotAt: opts.today,
    maxBytesGb: 1500,
    reuseExports: opts.reuseExports,
    exportName: opts.exportName,
    ecosystems: opts.ecosystems,
  })

  const { fileNames: pkgFileNames, rowCounts: pkgRowCounts } = await listParquetFiles({
    gcsPrefix: pkgsExport.gcsPrefix,
  })
  const pkgTotalFiles = pkgFileNames.length

  if (pkgTotalFiles === 0) {
    await mergeStagingToTable({
      jobId: pkgsExport.jobId,
      mergeSql: [],
      tableNames: [],
      isFinal: true,
    })
    return
  }

  const pkgTotalRows = pkgRowCounts.reduce((a, b) => a + b, 0)
  const pkgFilesPerChunk =
    pkgTotalRows > 0
      ? Math.max(1, Math.round((ROWS_PER_CHUNK * pkgFileNames.length) / pkgTotalRows))
      : Math.min(pkgFileNames.length, 2)
  const pkgTotalChunks = Math.ceil(pkgFileNames.length / pkgFilesPerChunk)
  let pkgPriorRowsAffected = 0
  let pkgPriorStagingRows = 0
  const pkgPriorTableRowCounts: Record<string, number> = {}

  for (let chunkIndex = 0; chunkIndex < pkgTotalChunks; chunkIndex++) {
    const start = chunkIndex * pkgFilesPerChunk
    const chunk = pkgFileNames.slice(start, start + pkgFilesPerChunk)
    const isFinal = chunkIndex === pkgTotalChunks - 1

    const { rowsLoaded } = await gcsParquetToStaging({
      jobId: pkgsExport.jobId,
      stagingTable: ADVISORY_PACKAGES_STAGING_TABLE,
      stagingDdl: ADVISORY_PACKAGES_STAGING_DDL,
      pgColumns: ADVISORY_PACKAGES_PG_COLUMNS,
      fileNames: chunk,
      filesOffset: start,
      totalFiles: pkgTotalFiles,
      priorStagingRows: pkgPriorStagingRows,
    })
    pkgPriorStagingRows += rowsLoaded

    const { rowsAffected, tableRowCounts } = await mergeStagingToTable({
      jobId: pkgsExport.jobId,
      mergeSql: [ADVISORY_PACKAGES_MERGE_SQL, ADVISORY_AFFECTED_RANGES_MERGE_SQL],
      tableNames: ['advisory_packages', 'advisory_affected_ranges'],
      isFinal,
      priorRowsAffected: pkgPriorRowsAffected,
      priorTableRowCounts: pkgPriorTableRowCounts,
      chunkInfo: { index: chunkIndex, total: pkgTotalChunks },
    })

    pkgPriorRowsAffected += rowsAffected
    if (!isFinal) {
      for (const [k, v] of Object.entries(tableRowCounts)) {
        pkgPriorTableRowCounts[k] = (pkgPriorTableRowCounts[k] ?? 0) + v
      }
    }
  }
}
