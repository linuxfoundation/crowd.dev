import { proxyActivities } from '@temporalio/workflow'

import {
  claimFromRow,
  competingGithubRepoExpr,
  packageRepoConfidenceCall,
} from '@crowd/data-access-layer/src/packages/repoConfidence'

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

// last_synced_at is intentionally left NULL on seed — it is owned by the GitHub
// enricher as its freshness signal. created_at / updated_at use their column defaults.
const REPOS_MERGE_SQL = `
INSERT INTO repos (url, raw_project_type, raw_project_name, host, owner, name,
                   description, homepage, stars, forks, open_issues)
SELECT s.canonical_url, s.raw_project_type, s.raw_project_name, s.host, s.owner, s.name,
       s.description, s.homepage, s.stars, s.forks, s.open_issues
FROM staging.osspckgs_repos_raw s
ON CONFLICT (url) DO NOTHING
`

const PKGREPOS_STAGING_TABLE = 'staging.osspckgs_package_repos_raw'

// Dropped rather than IF NOT EXISTS: the column set changed (confidence → provenance)
// in CM-1306 and the table is unlogged and truncated on every chunk anyway.
const PKGREPOS_STAGING_DDL = [
  `DROP TABLE IF EXISTS staging.osspckgs_package_repos_raw`,
  `CREATE UNLOGGED TABLE staging.osspckgs_package_repos_raw (
  purl          text,
  canonical_url text,
  provenance    text
)`,
]

const PKGREPOS_PG_COLUMNS = ['purl', 'canonical_url', 'provenance']

// DISTINCT ON picks the highest-scoring row per (package, repo) pair.
// packages must already be loaded (ingestPackages runs before ingestRepos in bootstrapOsspckgs).
// packages.purl is version-stripped by buildPackagesFullSql (REGEXP_REPLACE in BQ).
// Staging purl may or may not include @version depending on when the GCS export was taken,
// so strip on staging side only — packages.purl stays bare and the UNIQUE index is usable.

// github_staged: package IDs that have at least one GitHub-hosted repo in this chunk.
// Precomputed once per INSERT so the per-row competing_github check is a simple join
// rather than a correlated scan of the million-row staging table (O(n) not O(n²)).
// After all chunks are merged, rescore all non-GitHub links for packages that appeared in
// this ingest run and now have a competing GitHub repo. Scoped to staging package IDs so a
// single-ecosystem run does not touch unrelated rows in the full table.
// The competing-GitHub penalty applies to every source (declared, deps_dev, etc.) so all
// non-GitHub links for affected packages need refreshing, not just deps_dev ones.
const PKGREPOS_RESCORE_SQL = `
UPDATE package_repos pr
   SET confidence = s.confidence
  FROM packages p, repos r,
       LATERAL (
         SELECT ${packageRepoConfidenceCall('p', 'r', claimFromRow('pr'), competingGithubRepoExpr('p.id', 'r.id'))} AS confidence
       ) s
 WHERE p.id = pr.package_id
   AND r.id = pr.repo_id
   AND r.host <> 'github'
   AND ${competingGithubRepoExpr('p.id', 'r.id')}
   AND s.confidence IS DISTINCT FROM pr.confidence
   AND p.id IN (
     SELECT DISTINCT p2.id
       FROM staging.osspckgs_package_repos_raw s2
       JOIN packages p2 ON p2.purl = REGEXP_REPLACE(s2.purl, '@[^@]+$', '')
   )
`

const PKGREPOS_MERGE_SQL = `
WITH github_staged AS MATERIALIZED (
  SELECT DISTINCT p2.id AS package_id
  FROM staging.osspckgs_package_repos_raw s2
  JOIN repos r2 ON r2.url = s2.canonical_url
  JOIN packages p2 ON p2.purl = REGEXP_REPLACE(s2.purl, '@[^@]+$', '')
  WHERE r2.host = 'github'
)
INSERT INTO package_repos (
  package_id, repo_id, source, signal, ownership_match, provenance,
  confidence, verified_at, created_at
)
SELECT DISTINCT ON (p.id, r.id)
  p.id, r.id, 'deps_dev', 'primary', 'no_evidence', s.provenance,
  c.confidence, NOW(), NOW()
FROM staging.osspckgs_package_repos_raw s
JOIN packages p ON p.purl = REGEXP_REPLACE(s.purl, '@[^@]+$', '')
JOIN repos r ON r.url = s.canonical_url
CROSS JOIN LATERAL (
  SELECT ${packageRepoConfidenceCall(
    'p',
    'r',
    {
      source: `'deps_dev'`,
      signal: `'primary'`,
      ownershipMatch: `'no_evidence'`,
      provenance: 's.provenance',
    },
    `(${competingGithubRepoExpr('p.id', 'r.id')} OR (EXISTS (SELECT 1 FROM github_staged gs WHERE gs.package_id = p.id) AND r.host <> 'github'))`,
  )} AS confidence
) c
ORDER BY p.id, r.id, c.confidence DESC
ON CONFLICT (package_id, repo_id) DO UPDATE SET
  source           = CASE WHEN EXCLUDED.confidence > package_repos.confidence
                          THEN EXCLUDED.source ELSE package_repos.source END,
  signal           = CASE WHEN EXCLUDED.confidence > package_repos.confidence
                          THEN EXCLUDED.signal ELSE package_repos.signal END,
  ownership_match  = CASE WHEN EXCLUDED.confidence > package_repos.confidence
                          THEN EXCLUDED.ownership_match ELSE package_repos.ownership_match END,
  provenance       = CASE WHEN EXCLUDED.confidence > package_repos.confidence
                          THEN EXCLUDED.provenance ELSE package_repos.provenance END,
  confidence       = GREATEST(EXCLUDED.confidence, package_repos.confidence),
  verified_at      = NOW()
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
    ecosystems: opts.ecosystems,
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
    ecosystems: opts.ecosystems,
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
      mergeSql: isFinal ? [PKGREPOS_MERGE_SQL, PKGREPOS_RESCORE_SQL] : PKGREPOS_MERGE_SQL,
      tableNames: isFinal ? ['package_repos', 'package_repos'] : 'package_repos',
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
