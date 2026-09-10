import { ActivityFailure, ApplicationFailure, proxyActivities } from '@temporalio/workflow'

import type * as depsDevActivities from '../activities'
import { ADVISORIES_SQL, buildAdvisoryPackagesSql } from '../queries/advisoriesSql'
import { packageNameSplitSql } from '../queries/pgIdentity'
import { toSystemsFilter } from '../queries/systems'

const { namespace: NAMESPACE_SPLIT_SQL, name: NAME_SPLIT_SQL } = packageNameSplitSql(
  'r',
  'package_name',
)

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

// bqExportToGcs throws ApplicationFailure.nonRetryable('BQ_CEILING_EXCEEDED') directly from
// activity code, which the SDK surfaces here as ActivityFailure.cause. Rethrow as a workflow-level
// ApplicationFailure carrying the job kind as a detail so bootstrapOsspckgs's unwrap (err.cause on
// the resulting ChildWorkflowFailure) can match it, soft-fail, and alert on whichever export
// actually breached — mirroring the DEPENDENT_COUNTS_GUARD / EDGE_SNAPSHOT_GUARD pattern there.
// Applies to both exports below (review comment on CM-1362): a ceiling breach on either one must
// be recognized, not just advisory_packages.
async function exportWithCeilingGuard(input: Parameters<typeof bqExportToGcs>[0]) {
  try {
    return await bqExportToGcs(input)
  } catch (err) {
    const cause = err instanceof ActivityFailure ? err.cause : err
    if (cause instanceof ApplicationFailure && cause.type === 'BQ_CEILING_EXCEEDED') {
      throw ApplicationFailure.nonRetryable(cause.message, 'BQ_CEILING_EXCEEDED', input.jobKind)
    }
    throw err
  }
}

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

// Two-statement DDL: DROP before CREATE so a deployed table with the old `purl` column doesn't
// silently stick around under `CREATE ... IF NOT EXISTS`. Staging is TRUNCATED/recreated every run.
const ADVISORY_PACKAGES_STAGING_DDL = [
  `DROP TABLE IF EXISTS staging.osspckgs_advisory_packages_raw`,
  `CREATE UNLOGGED TABLE staging.osspckgs_advisory_packages_raw (
  osv_id         text,
  ecosystem      text,
  package_name   text,
  range_raw      text,
  unaffected_raw text
)`,
]

const ADVISORIES_MERGE_SQL = `
INSERT INTO advisories (osv_id, source, source_url, summary, details, cvss, severity, aliases, published_at, created_at, updated_at)
SELECT osv_id, source, source_url, summary, details,
  CASE WHEN cvss IS NULL OR cvss = 'NaN'::float8 OR cvss = 'Infinity'::float8 OR cvss = '-Infinity'::float8
    THEN NULL ELSE cvss::numeric(3,1) END,
  severity, aliases, published_at, NOW(), NOW()
FROM staging.osspckgs_advisories_raw
ON CONFLICT (osv_id) DO NOTHING
`

// package_id is resolved here by reconstructing the same (ecosystem, namespace, name) identity
// ingestPackages.ts writes into `packages` (packageNameSplitSql, shared by both), rather than by
// a BQ-sourced purl (CM-1362 — the purl_map scan cost ~1.5 TB per run for a join key we already
// had locally). COALESCE(p.namespace,'') mirrors the unique index expression verbatim, so this
// stays an index lookup. Still a LEFT JOIN: package_id stays nullable, resolveMissingPackageIds
// keeps its catch-up role for anything unresolved here.
const ADVISORY_PACKAGES_MERGE_SQL = `
WITH s AS (
  SELECT r.osv_id, r.ecosystem, r.package_name,
    ${NAMESPACE_SPLIT_SQL} AS namespace,
    ${NAME_SPLIT_SQL} AS name
  FROM staging.osspckgs_advisory_packages_raw r
)
INSERT INTO advisory_packages (advisory_id, package_id, ecosystem, package_name, created_at, updated_at)
SELECT
  adv.id,
  p.id,
  s.ecosystem,
  s.package_name,
  NOW(), NOW()
FROM s
JOIN advisories adv ON adv.osv_id = s.osv_id
LEFT JOIN packages p
  ON p.ecosystem = s.ecosystem
 AND COALESCE(p.namespace, '') = COALESCE(s.namespace, '')
 AND p.name = s.name
ON CONFLICT (advisory_id, ecosystem, package_name) DO NOTHING
`

// Separate statement — must execute after ADVISORY_PACKAGES_MERGE_SQL so advisory_packages rows exist.
// Skips any advisory_package that already has a live OSV-owned range: OSV is the
// source of truth over deps.dev for overlapping advisory_packages (ADR-0001
// §advisory_affected_ranges delete/dedup strategy), and this merge runs on its own
// BQ-driven schedule, independent of and potentially after an OSV sync — the
// per-tuple ON CONFLICT below can't see that, since a NULL-bounds raw tuple has a
// different key than OSV's structured tuple and would insert as a new live
// duplicate. The NOT EXISTS guard makes the ownership rule package-level instead
// of tuple-level, so deps.dev never adds a live row once OSV owns the package.
// ON CONFLICT revives (not skips) a soft-deleted row occupying the same tuple —
// typical after supersedeDepsDevRanges soft-deletes deps.dev rows on OSV takeover
// and OSV later drops the package again — otherwise DO NOTHING would leave
// staging's live data with no corresponding live row.
// DISTINCT ON (ap.id) is required because DO UPDATE (unlike DO NOTHING) errors
// with "ON CONFLICT DO UPDATE command cannot affect row a second time" if two
// staging rows for the same package (e.g. multiple disjoint deps.dev ranges
// under one advisory) hit the same conflict target within one statement — every
// deps.dev row shares one key per advisory_package_id since introduced_version
// etc. are always NULL here.
const ADVISORY_AFFECTED_RANGES_MERGE_SQL = `
INSERT INTO advisory_affected_ranges (advisory_package_id, range_raw, unaffected_raw, introduced_version, created_at, updated_at)
SELECT DISTINCT ON (ap.id)
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
WHERE NOT EXISTS (
  SELECT 1 FROM advisory_affected_ranges live
  WHERE live.advisory_package_id = ap.id
    AND live.deleted_at IS NULL
    AND live.range_raw IS NULL
    AND live.unaffected_raw IS NULL
)
ORDER BY ap.id
ON CONFLICT (advisory_package_id, COALESCE(introduced_version, ''), COALESCE(fixed_version, ''), COALESCE(last_affected, ''))
DO UPDATE SET
  updated_at = NOW(),
  deleted_at = NULL,
  range_raw = EXCLUDED.range_raw,
  unaffected_raw = EXCLUDED.unaffected_raw
WHERE advisory_affected_ranges.deleted_at IS NOT NULL
`

// Runs as prepareSql (uncounted) ahead of ADVISORY_AFFECTED_RANGES_MERGE_SQL, in the
// same transaction. Row-locks every advisory_packages this chunk will touch, in id
// order, before the NOT EXISTS ownership check runs. OSV's upsertOne (services/apps/
// packages_worker/src/osv/upsertAdvisory.ts) takes the matching per-row lock before it
// writes advisory_affected_ranges for that advisory_package, so whichever transaction
// (deps.dev chunk or OSV record) locks the row first now forces the other to wait and
// see its committed writes — closing the race the two independently-scheduled write
// paths would otherwise have on the ownership check (ADR-0001 §Write semantics).
const ADVISORY_PACKAGES_LOCK_SQL = `
SELECT ap.id
FROM advisory_packages ap
JOIN staging.osspckgs_advisory_packages_raw s
  ON s.ecosystem = ap.ecosystem AND s.package_name = ap.package_name
JOIN advisories adv ON adv.osv_id = s.osv_id AND adv.id = ap.advisory_id
ORDER BY ap.id
FOR UPDATE OF ap
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
  const advisoriesExport = await exportWithCeilingGuard({
    jobKind: 'advisories',
    sql: ADVISORIES_SQL,
    runId: opts.runId,
    syncMode: opts.syncMode,
    snapshotAt: opts.today,
    maxBytesGb: 20,
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
  const pkgsExport = await exportWithCeilingGuard({
    jobKind: 'advisory_packages',
    sql: buildAdvisoryPackagesSql(systems),
    runId: opts.runId,
    syncMode: opts.syncMode,
    snapshotAt: opts.today,
    // No purl_map (CM-1362): measured actual scan is ~1.4 GB against AdvisoriesLatest; 50 GB
    // keeps this a real regression gate instead of a ceiling that periodically needs raising.
    maxBytesGb: 50,
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
      prepareSql: ADVISORY_PACKAGES_LOCK_SQL,
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
