import { proxyActivities } from '@temporalio/workflow'

import type * as depsDevActivities from '../activities'
import { buildPackageReposSql } from '../queries/packageReposSql'
import { buildReposSql } from '../queries/reposSql'
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
  startToCloseTimeout: '2 hours',
  heartbeatTimeout: '2 minutes',
  retry: { maximumAttempts: 2 },
})

const { mergeStagingToTable } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '30 minutes',
  retry: { maximumAttempts: 1 },
})

const REPOS_STAGING_TABLE = 'staging.osspckgs_repos_raw'

const REPOS_STAGING_DDL = `
CREATE UNLOGGED TABLE IF NOT EXISTS staging.osspckgs_repos_raw (
  canonical_url    text,
  host             text,
  owner            text,
  name             text,
  raw_project_type text,
  raw_project_name text,
  description      text,
  homepage         text,
  stars            int,
  forks            int,
  open_issues      int
)
`

const REPOS_PG_COLUMNS = [
  'canonical_url',
  'host',
  'owner',
  'name',
  'raw_project_type',
  'raw_project_name',
  'description',
  'homepage',
  'stars',
  'forks',
  'open_issues',
]

const REPOS_MERGE_SQL = `
INSERT INTO repos (url, raw_project_type, raw_project_name, host, owner, name,
                   description, homepage, stars, forks, open_issues, last_synced_at)
SELECT s.canonical_url, s.raw_project_type, s.raw_project_name, s.host, s.owner, s.name,
       s.description, s.homepage, s.stars, s.forks, s.open_issues, NOW()
FROM staging.osspckgs_repos_raw s
ON CONFLICT (url) DO NOTHING
`

const PKGREPOS_STAGING_TABLE = 'staging.osspckgs_package_repos_raw'

const PKGREPOS_STAGING_DDL = `
CREATE UNLOGGED TABLE IF NOT EXISTS staging.osspckgs_package_repos_raw (
  purl          text,
  canonical_url text,
  confidence    float8
)
`

const PKGREPOS_PG_COLUMNS = ['purl', 'canonical_url', 'confidence']

// DISTINCT ON picks the highest-confidence row per (package, repo) pair.
// packages must already be loaded (ingestPackages runs before ingestRepos in bootstrapOsspckgs).
// packages.purl is version-stripped by buildPackagesFullSql (REGEXP_REPLACE in BQ).
// Staging purl may or may not include @version depending on when the GCS export was taken,
// so strip on staging side only — packages.purl stays bare and the UNIQUE index is usable.
const PKGREPOS_MERGE_SQL = `
INSERT INTO package_repos (package_id, repo_id, source, confidence, verified_at)
SELECT DISTINCT ON (p.id, r.id)
  p.id, r.id, 'deps_dev', s.confidence, NOW()
FROM staging.osspckgs_package_repos_raw s
JOIN packages p ON p.purl = REGEXP_REPLACE(s.purl, '@[^@]+$', '')
JOIN repos r ON r.url = s.canonical_url
ORDER BY p.id, r.id, s.confidence DESC
ON CONFLICT (package_id, repo_id) DO NOTHING
`

const ROWS_PER_CHUNK = 1_000_000

export async function ingestRepos(opts: {
  runId: string
  snapshotDate: string
  ecosystems?: string[]
  reuseExports?: boolean
  exportName?: string
}): Promise<void> {
  const systems = toSystemsFilter(opts.ecosystems)

  const reposExport = await bqExportToGcs({
    jobKind: 'repos',
    sql: buildReposSql(opts.snapshotDate, systems),
    runId: opts.runId,
    syncMode: 'full',
    snapshotAt: opts.snapshotDate,
    maxBytesGb: 2000,
    reuseExports: opts.reuseExports,
    exportName: opts.exportName,
  })

  const { fileNames: repoFileNames, rowCounts: repoRowCounts } = await listParquetFiles({
    gcsPrefix: reposExport.gcsPrefix,
  })
  const repoTotalFiles = repoFileNames.length

  if (repoTotalFiles === 0) {
    await mergeStagingToTable({
      jobId: reposExport.jobId,
      mergeSql: [],
      tableNames: [],
      isFinal: true,
    })
  } else {
    const repoTotalRows = repoRowCounts.reduce((a, b) => a + b, 0)
    const repoFilesPerChunk =
      repoTotalRows > 0
        ? Math.max(1, Math.round((ROWS_PER_CHUNK * repoFileNames.length) / repoTotalRows))
        : Math.min(repoFileNames.length, 2)
    const repoTotalChunks = Math.ceil(repoFileNames.length / repoFilesPerChunk)
    let priorRowsAffected = 0
    let repoPriorStagingRows = 0
    const repoPriorTableRowCounts: Record<string, number> = {}

    for (let chunkIndex = 0; chunkIndex < repoTotalChunks; chunkIndex++) {
      const start = chunkIndex * repoFilesPerChunk
      const chunk = repoFileNames.slice(start, start + repoFilesPerChunk)
      const isFinal = chunkIndex === repoTotalChunks - 1

      const { rowsLoaded } = await gcsParquetToStaging({
        jobId: reposExport.jobId,
        stagingTable: REPOS_STAGING_TABLE,
        stagingDdl: REPOS_STAGING_DDL,
        pgColumns: REPOS_PG_COLUMNS,
        fileNames: chunk,
        filesOffset: start,
        totalFiles: repoTotalFiles,
        priorStagingRows: repoPriorStagingRows,
      })
      repoPriorStagingRows += rowsLoaded

      const { rowsAffected, tableRowCounts } = await mergeStagingToTable({
        jobId: reposExport.jobId,
        mergeSql: REPOS_MERGE_SQL,
        tableNames: 'repos',
        isFinal,
        priorRowsAffected,
        priorTableRowCounts: repoPriorTableRowCounts,
        chunkInfo: { index: chunkIndex, total: repoTotalChunks },
      })

      priorRowsAffected += rowsAffected
      if (!isFinal) {
        for (const [k, v] of Object.entries(tableRowCounts)) {
          repoPriorTableRowCounts[k] = (repoPriorTableRowCounts[k] ?? 0) + v
        }
      }
    }
  }

  const pkgReposExport = await bqExportToGcs({
    jobKind: 'package_repos',
    sql: buildPackageReposSql(opts.snapshotDate, systems),
    runId: opts.runId,
    syncMode: 'full',
    snapshotAt: opts.snapshotDate,
    maxBytesGb: 2000,
    reuseExports: opts.reuseExports,
    exportName: opts.exportName,
  })

  const { fileNames: pkgRepoFileNames, rowCounts: pkgRepoRowCounts } = await listParquetFiles({
    gcsPrefix: pkgReposExport.gcsPrefix,
  })
  const pkgRepoTotalFiles = pkgRepoFileNames.length

  if (pkgRepoTotalFiles === 0) {
    await mergeStagingToTable({
      jobId: pkgReposExport.jobId,
      mergeSql: [],
      tableNames: [],
      isFinal: true,
    })
    return
  }

  const pkgRepoTotalRows = pkgRepoRowCounts.reduce((a, b) => a + b, 0)
  const pkgRepoFilesPerChunk =
    pkgRepoTotalRows > 0
      ? Math.max(1, Math.round((ROWS_PER_CHUNK * pkgRepoFileNames.length) / pkgRepoTotalRows))
      : Math.min(pkgRepoFileNames.length, 2)
  const pkgRepoTotalChunks = Math.ceil(pkgRepoFileNames.length / pkgRepoFilesPerChunk)
  let pkgRepoPriorRowsAffected = 0
  let pkgRepoPriorStagingRows = 0
  const pkgRepoPriorTableRowCounts: Record<string, number> = {}

  for (let chunkIndex = 0; chunkIndex < pkgRepoTotalChunks; chunkIndex++) {
    const start = chunkIndex * pkgRepoFilesPerChunk
    const chunk = pkgRepoFileNames.slice(start, start + pkgRepoFilesPerChunk)
    const isFinal = chunkIndex === pkgRepoTotalChunks - 1

    const { rowsLoaded } = await gcsParquetToStaging({
      jobId: pkgReposExport.jobId,
      stagingTable: PKGREPOS_STAGING_TABLE,
      stagingDdl: PKGREPOS_STAGING_DDL,
      pgColumns: PKGREPOS_PG_COLUMNS,
      fileNames: chunk,
      filesOffset: start,
      totalFiles: pkgRepoTotalFiles,
      priorStagingRows: pkgRepoPriorStagingRows,
    })
    pkgRepoPriorStagingRows += rowsLoaded

    const { rowsAffected, tableRowCounts } = await mergeStagingToTable({
      jobId: pkgReposExport.jobId,
      mergeSql: PKGREPOS_MERGE_SQL,
      tableNames: 'package_repos',
      isFinal,
      priorRowsAffected: pkgRepoPriorRowsAffected,
      priorTableRowCounts: pkgRepoPriorTableRowCounts,
      chunkInfo: { index: chunkIndex, total: pkgRepoTotalChunks },
    })

    pkgRepoPriorRowsAffected += rowsAffected
    if (!isFinal) {
      for (const [k, v] of Object.entries(tableRowCounts)) {
        pkgRepoPriorTableRowCounts[k] = (pkgRepoPriorTableRowCounts[k] ?? 0) + v
      }
    }
  }
}
