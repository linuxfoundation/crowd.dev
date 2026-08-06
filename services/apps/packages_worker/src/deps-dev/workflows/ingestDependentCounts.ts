import { ApplicationFailure, proxyActivities } from '@temporalio/workflow'

import type { OsspckgsJobKind } from '@crowd/data-access-layer'

import type * as depsDevActivities from '../activities'
import {
  buildDependentCountsSql,
  buildGoDependentCountsSql,
  buildNugetDependentCountsSql,
  buildRubygemsDependentCountsSql,
} from '../queries/dependentCountsSql'

const { bqExportToGcs } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '1 hour',
  retry: { maximumAttempts: 3, initialInterval: '1 minute', backoffCoefficient: 2 },
})

const { listParquetFiles } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '5 minutes',
  retry: { maximumAttempts: 3 },
})

const { gcsParquetToStaging } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '2 hours',
  heartbeatTimeout: '2 minutes',
  retry: { maximumAttempts: 2 },
})

const { mergeStagingToTable } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '1 hour',
  retry: { maximumAttempts: 1 },
})

const { checkDependentCountsGuard } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 3 },
})

const { setJobStep } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '30 seconds',
  retry: { maximumAttempts: 3 },
})

// The dependent-counts ingest runs four independent ways, one per source of reverse-dependent data:
//   - 'edges'    : NPM/MAVEN/PYPI/CARGO from the deps.dev `Dependents` reverse index (single SELECT)
//   - 'go'       : GO from the GoRequirementsLatest exact reverse transitive closure (BQ script)
//   - 'nuget'    : NUGET from the NuGetRequirementsLatest exact reverse transitive closure (BQ script)
//   - 'rubygems' : RUBYGEMS from the RubyGemsRequirementsLatest exact reverse transitive closure (BQ script)
// All four produce the same three columns (dependent_count, transitive_dependent_count,
// dependent_repos_count) — deps.dev gives the edge systems their transitive graph directly, while
// GO/NUGET/RUBYGEMS compute it via the semi-naive closure script (isScript, see ADR-0004 +
// dependentCountsSql). Each way is its own job kind, staging table, guard baseline, and purl-disjoint
// merge — so a corrupt edge snapshot aborts only the edge way and the manifest-sourced counts
// (unaffected) still update. 'edges' keeps the original `dependent_counts` kind so existing job
// history / monitor / guard baseline stay continuous.
export type DependentCountsVariant = 'edges' | 'go' | 'nuget' | 'rubygems'

interface VariantConfig {
  jobKind: OsspckgsJobKind
  stagingTable: string
  stagingDdl: string[]
  mergeSql: string
  pgColumns: string[]
  maxBytesGb: number
  buildSql: (snapshotDate: string) => string
  // When true, buildSql returns a multi-statement BQ script (the GO/NUGET/RUBYGEMS exact reverse
  // transitive closure) that ends by creating TEMP TABLE _export_data, rather than a single SELECT.
  // The export activity then appends only EXPORT DATA and enforces the byte ceiling via
  // maximumBytesBilled instead of a dry-run. See ADR-0004. Unset (single-SELECT) for the edges
  // variant; set for GO/NUGET/RUBYGEMS.
  isScript?: boolean
}

// Two-statement DDL: DROP before CREATE so an existing table with stale columns doesn't block the
// new schema. Staging table is TRUNCATED/recreated on every run anyway.
const EDGES_STAGING = 'staging.osspckgs_dependent_counts_raw'
const GO_STAGING = 'staging.osspckgs_dependent_counts_go_raw'
const NUGET_STAGING = 'staging.osspckgs_dependent_counts_nuget_raw'
const RUBYGEMS_STAGING = 'staging.osspckgs_dependent_counts_rubygems_raw'

// All four ways now produce the same shape: purl + the three count columns. Each variant has its own
// staging table (parallel-safe) but identical DDL/merge; the merges touch disjoint purl spaces (one
// per ecosystem), so they never collide.
const PG_COLUMNS = [
  'purl',
  'dependent_count',
  'transitive_dependent_count',
  'dependent_repos_count',
]

const stagingDdl = (table: string): string[] => [
  `DROP TABLE IF EXISTS ${table}`,
  `CREATE UNLOGGED TABLE ${table} (
  purl                       text,
  dependent_count            bigint,
  transitive_dependent_count bigint,
  dependent_repos_count      bigint
)`,
]

// Strip @version from the staging purl — BQ ANY_VALUE(Purl) is non-deterministic across separate
// query executions and may include or omit the version suffix.
const dependentCountsMerge = (staging: string): string => `
UPDATE packages SET
  dependent_count            = s.dependent_count,
  transitive_dependent_count = s.transitive_dependent_count,
  dependent_repos_count      = s.dependent_repos_count,
  last_synced_at             = NOW()
FROM ${staging} s
WHERE packages.purl = REGEXP_REPLACE(s.purl, '@[^@]+$', '')
`

const VARIANTS: Record<DependentCountsVariant, VariantConfig> = {
  edges: {
    jobKind: 'dependent_counts',
    stagingTable: EDGES_STAGING,
    stagingDdl: stagingDdl(EDGES_STAGING),
    mergeSql: dependentCountsMerge(EDGES_STAGING),
    pgColumns: PG_COLUMNS,
    maxBytesGb: 2000,
    buildSql: (snapshotDate) => buildDependentCountsSql(snapshotDate),
  },
  // GO/NUGET/RUBYGEMS run the exact reverse transitive closure script (isScript). maxBytesGb is
  // the server-side maximumBytesBilled runaway cap, set well above the validated full-pipeline
  // spend (GO 2.31 TB incl. the all-depth repos aggregation; NUGET ~32 GB) so a normal week never
  // trips it — the iteration cap inside the script is the deterministic guard. Env-overridable via
  // BQ_DATASET_INGEST_DEPENDENT_COUNTS_GO_MAX_BQ_GB / _NUGET_ / _RUBYGEMS_.
  go: {
    jobKind: 'dependent_counts_go',
    stagingTable: GO_STAGING,
    stagingDdl: stagingDdl(GO_STAGING),
    mergeSql: dependentCountsMerge(GO_STAGING),
    pgColumns: PG_COLUMNS,
    maxBytesGb: 5000,
    buildSql: (snapshotDate) => buildGoDependentCountsSql(snapshotDate),
    isScript: true,
  },
  nuget: {
    jobKind: 'dependent_counts_nuget',
    stagingTable: NUGET_STAGING,
    stagingDdl: stagingDdl(NUGET_STAGING),
    mergeSql: dependentCountsMerge(NUGET_STAGING),
    pgColumns: PG_COLUMNS,
    maxBytesGb: 200,
    buildSql: (snapshotDate) => buildNugetDependentCountsSql(snapshotDate),
    isScript: true,
  },
  // RubyGems: same shape as NUGET (manifest-sourced closure), similarly small corpus
  // (~1.7M pkgs / ~4.5M runtime edges — verified via BQ 2026-07-13).
  rubygems: {
    jobKind: 'dependent_counts_rubygems',
    stagingTable: RUBYGEMS_STAGING,
    stagingDdl: stagingDdl(RUBYGEMS_STAGING),
    mergeSql: dependentCountsMerge(RUBYGEMS_STAGING),
    pgColumns: PG_COLUMNS,
    maxBytesGb: 200,
    buildSql: (snapshotDate) => buildRubygemsDependentCountsSql(snapshotDate),
    isScript: true,
  },
}

const ROWS_PER_CHUNK = 1_000_000

export async function ingestDependentCounts(opts: {
  runId: string
  snapshotDate: string
  reuseExports?: boolean
  exportName?: string
  variant?: DependentCountsVariant // defaults to 'edges' for backward compatibility
}): Promise<void> {
  const cfg = VARIANTS[opts.variant ?? 'edges']

  const exportResult = await bqExportToGcs({
    jobKind: cfg.jobKind,
    sql: cfg.buildSql(opts.snapshotDate),
    runId: opts.runId,
    syncMode: 'full',
    snapshotAt: opts.snapshotDate,
    maxBytesGb: cfg.maxBytesGb,
    reuseExports: opts.reuseExports,
    exportName: opts.exportName,
    isScript: cfg.isScript,
  })

  const { fileNames, rowCounts } = await listParquetFiles({ gcsPrefix: exportResult.gcsPrefix })
  const totalFiles = fileNames.length
  const totalRows = totalFiles > 0 ? rowCounts.reduce((a, b) => a + b, 0) : 0

  await setJobStep({ jobId: exportResult.jobId, step: 'guard_check' })
  const guard = await checkDependentCountsGuard({
    currentRowCount: totalRows,
    snapshotDate: opts.snapshotDate,
    jobKind: cfg.jobKind,
  })
  if (!guard.ok) {
    throw ApplicationFailure.nonRetryable(
      `${cfg.jobKind} guard failed: ${String(totalRows)} rows vs prev max ${String(guard.prevRowCount)} ` +
        `(${((guard.dropPct ?? 0) * 100).toFixed(1)}% drop). Slack alert sent. Aborting to preserve existing data.`,
      'DEPENDENT_COUNTS_GUARD',
    )
  }

  if (totalFiles === 0) {
    await mergeStagingToTable({
      jobId: exportResult.jobId,
      mergeSql: [],
      tableNames: [],
      isFinal: true,
    })
    return
  }

  const filesPerChunk =
    totalRows > 0
      ? Math.max(1, Math.round((ROWS_PER_CHUNK * fileNames.length) / totalRows))
      : Math.min(fileNames.length, 2)
  const totalChunks = Math.ceil(fileNames.length / filesPerChunk)
  let priorRowsAffected = 0
  let priorStagingRows = 0
  const priorTableRowCounts: Record<string, number> = {}

  for (let chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++) {
    const start = chunkIndex * filesPerChunk
    const chunk = fileNames.slice(start, start + filesPerChunk)
    const isFinal = chunkIndex === totalChunks - 1

    const { rowsLoaded } = await gcsParquetToStaging({
      jobId: exportResult.jobId,
      stagingTable: cfg.stagingTable,
      stagingDdl: cfg.stagingDdl,
      pgColumns: cfg.pgColumns,
      fileNames: chunk,
      filesOffset: start,
      totalFiles,
      priorStagingRows,
    })
    priorStagingRows += rowsLoaded

    const { rowsAffected, tableRowCounts } = await mergeStagingToTable({
      jobId: exportResult.jobId,
      mergeSql: cfg.mergeSql,
      tableNames: 'packages',
      isFinal,
      priorRowsAffected,
      priorTableRowCounts,
      chunkInfo: { index: chunkIndex, total: totalChunks },
    })

    priorRowsAffected += rowsAffected
    if (!isFinal) {
      for (const [k, v] of Object.entries(tableRowCounts)) {
        priorTableRowCounts[k] = (priorTableRowCounts[k] ?? 0) + v
      }
    }
  }
}
