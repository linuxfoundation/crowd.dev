import { proxyActivities } from '@temporalio/workflow'

import type * as depsDevActivities from '../../deps-dev/activities'
import { SCORECARD_CHECKS_SQL, SCORECARD_REPOS_SQL } from '../queries/scorecardSql'

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

const SCORECARD_REPOS_STAGING_TABLE = 'staging.osspckgs_scorecard_repos_raw'

const SCORECARD_REPOS_STAGING_DDL = `
CREATE UNLOGGED TABLE IF NOT EXISTS staging.osspckgs_scorecard_repos_raw (
  repo_url   text,
  score      float8,
  scanned_at text
)
`

// scanned_at is text because BQ DATE → Parquet INT32 DATE → JS Date; pg serialises it as ISO string.
// Cast to timestamptz happens in merge SQL.
const SCORECARD_REPOS_PG_COLUMNS = ['repo_url', 'score', 'scanned_at']

const SCORECARD_REPOS_MERGE_SQL = `
UPDATE repos r
SET scorecard_score = CASE
      WHEN s.score IS NULL
        OR s.score = 'NaN'::float8
        OR s.score = 'Infinity'::float8
        OR s.score = '-Infinity'::float8
      THEN NULL
      ELSE s.score::numeric(3,1)
    END,
    scorecard_last_run_at = s.scanned_at::timestamptz,
    updated_at = NOW()
FROM staging.osspckgs_scorecard_repos_raw s
WHERE r.url = s.repo_url
`

const SCORECARD_CHECKS_STAGING_TABLE = 'staging.osspckgs_scorecard_checks_raw'

const SCORECARD_CHECKS_STAGING_DDL = `
CREATE UNLOGGED TABLE IF NOT EXISTS staging.osspckgs_scorecard_checks_raw (
  repo_url     text,
  check_name   text,
  check_score  int,
  check_reason text
)
`

const SCORECARD_CHECKS_PG_COLUMNS = ['repo_url', 'check_name', 'check_score', 'check_reason']

const SCORECARD_CHECKS_MERGE_SQL = `
INSERT INTO repo_scorecard_checks (repo_id, check_name, score, reason)
SELECT r.id,
       s.check_name,
       NULLIF(s.check_score, -1)::numeric(3,1),
       s.check_reason
FROM staging.osspckgs_scorecard_checks_raw s
JOIN repos r ON r.url = s.repo_url
ON CONFLICT (repo_id, check_name) DO UPDATE SET
  score      = EXCLUDED.score,
  reason     = EXCLUDED.reason,
  updated_at = NOW()
`

const ROWS_PER_CHUNK = 1_000_000

export async function ingestScorecard(opts: {
  runId: string
  reuseExports?: boolean
  exportName?: string
}): Promise<void> {
  // Step 1: repos aggregate scores (plain UPDATE — repos already exist from deps-dev ingest)
  const reposExport = await bqExportToGcs({
    jobKind: 'scorecard_repos',
    sql: SCORECARD_REPOS_SQL,
    runId: opts.runId,
    syncMode: 'full',
    snapshotAt: null,
    maxBytesGb: 10,
    reuseExports: opts.reuseExports,
    exportName: opts.exportName,
  })

  const { fileNames: repoFileNames, rowCounts: repoRowCounts } = await listParquetFiles({
    gcsPrefix: reposExport.gcsPrefix,
  })
  const repoTotalFiles = repoFileNames.length

  if (repoTotalFiles === 0) {
    await mergeStagingToTable({ jobId: reposExport.jobId, mergeSql: [], tableNames: [], isFinal: true })
  } else {
    const repoTotalRows = repoRowCounts.reduce((a, b) => a + b, 0)
    const repoFilesPerChunk =
      repoTotalRows > 0
        ? Math.max(1, Math.round((ROWS_PER_CHUNK * repoFileNames.length) / repoTotalRows))
        : Math.min(repoFileNames.length, 2)
    const repoTotalChunks = Math.ceil(repoFileNames.length / repoFilesPerChunk)
    let repoPriorRowsAffected = 0
    let repoPriorStagingRows = 0
    const repoPriorTableRowCounts: Record<string, number> = {}

    for (let chunkIndex = 0; chunkIndex < repoTotalChunks; chunkIndex++) {
      const start = chunkIndex * repoFilesPerChunk
      const chunk = repoFileNames.slice(start, start + repoFilesPerChunk)
      const isFinal = chunkIndex === repoTotalChunks - 1

      const { rowsLoaded } = await gcsParquetToStaging({
        jobId: reposExport.jobId,
        stagingTable: SCORECARD_REPOS_STAGING_TABLE,
        stagingDdl: SCORECARD_REPOS_STAGING_DDL,
        pgColumns: SCORECARD_REPOS_PG_COLUMNS,
        fileNames: chunk,
        filesOffset: start,
        totalFiles: repoTotalFiles,
        priorStagingRows: repoPriorStagingRows,
      })
      repoPriorStagingRows += rowsLoaded

      const { rowsAffected, tableRowCounts } = await mergeStagingToTable({
        jobId: reposExport.jobId,
        mergeSql: SCORECARD_REPOS_MERGE_SQL,
        tableNames: 'repos',
        isFinal,
        priorRowsAffected: repoPriorRowsAffected,
        priorTableRowCounts: repoPriorTableRowCounts,
        chunkInfo: { index: chunkIndex, total: repoTotalChunks },
      })

      repoPriorRowsAffected += rowsAffected
      if (!isFinal) {
        for (const [k, v] of Object.entries(tableRowCounts)) {
          repoPriorTableRowCounts[k] = (repoPriorTableRowCounts[k] ?? 0) + v
        }
      }
    }
  }

  // Step 2: per-check detail (FK → repos, so must run after Step 1)
  const checksExport = await bqExportToGcs({
    jobKind: 'scorecard_checks',
    sql: SCORECARD_CHECKS_SQL,
    runId: opts.runId,
    syncMode: 'full',
    snapshotAt: null,
    maxBytesGb: 200,
    reuseExports: opts.reuseExports,
    exportName: opts.exportName,
  })

  const { fileNames: checkFileNames, rowCounts: checkRowCounts } = await listParquetFiles({
    gcsPrefix: checksExport.gcsPrefix,
  })
  const checkTotalFiles = checkFileNames.length

  if (checkTotalFiles === 0) {
    await mergeStagingToTable({ jobId: checksExport.jobId, mergeSql: [], tableNames: [], isFinal: true })
    return
  }

  const checkTotalRows = checkRowCounts.reduce((a, b) => a + b, 0)
  const checkFilesPerChunk =
    checkTotalRows > 0
      ? Math.max(1, Math.round((ROWS_PER_CHUNK * checkFileNames.length) / checkTotalRows))
      : Math.min(checkFileNames.length, 2)
  const checkTotalChunks = Math.ceil(checkFileNames.length / checkFilesPerChunk)
  let checkPriorRowsAffected = 0
  let checkPriorStagingRows = 0
  const checkPriorTableRowCounts: Record<string, number> = {}

  for (let chunkIndex = 0; chunkIndex < checkTotalChunks; chunkIndex++) {
    const start = chunkIndex * checkFilesPerChunk
    const chunk = checkFileNames.slice(start, start + checkFilesPerChunk)
    const isFinal = chunkIndex === checkTotalChunks - 1

    const { rowsLoaded } = await gcsParquetToStaging({
      jobId: checksExport.jobId,
      stagingTable: SCORECARD_CHECKS_STAGING_TABLE,
      stagingDdl: SCORECARD_CHECKS_STAGING_DDL,
      pgColumns: SCORECARD_CHECKS_PG_COLUMNS,
      fileNames: chunk,
      filesOffset: start,
      totalFiles: checkTotalFiles,
      priorStagingRows: checkPriorStagingRows,
    })
    checkPriorStagingRows += rowsLoaded

    const { rowsAffected, tableRowCounts } = await mergeStagingToTable({
      jobId: checksExport.jobId,
      mergeSql: SCORECARD_CHECKS_MERGE_SQL,
      tableNames: 'repo_scorecard_checks',
      isFinal,
      priorRowsAffected: checkPriorRowsAffected,
      priorTableRowCounts: checkPriorTableRowCounts,
      chunkInfo: { index: chunkIndex, total: checkTotalChunks },
    })

    checkPriorRowsAffected += rowsAffected
    if (!isFinal) {
      for (const [k, v] of Object.entries(tableRowCounts)) {
        checkPriorTableRowCounts[k] = (checkPriorTableRowCounts[k] ?? 0) + v
      }
    }
  }
}
