import { proxyActivities, workflowInfo } from '@temporalio/workflow'

import type * as depsDevActivities from '../../deps-dev/activities'

import type * as pypiDownloadsActivities from './getCriticalPypiCount'
import {
  computeLast30dWindows,
  defaultDailyRange,
  utcFirstOfCurrentMonth,
} from './pypiDownloadsDates'
import {
  PYPI_DOWNLOADS_30D_KIND,
  PYPI_DOWNLOADS_30D_STAGING,
  PYPI_DOWNLOADS_DAILY_KIND,
  PYPI_DOWNLOADS_DAILY_STAGING,
  buildPypiDownloads30dMergeSql,
  buildPypiDownloads30dSql,
  buildPypiDownloadsDailyMergeSql,
  buildPypiDownloadsDailySql,
} from './pypiDownloadsSql'

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

const { getCriticalPypiCount } = proxyActivities<typeof pypiDownloadsActivities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 3 },
})

// Per-kind ceilings guard against runaway BQ scans; override via
// BQ_DATASET_INGEST_PYPI_DOWNLOADS_{30D,DAILY}_MAX_BQ_GB. Defaults sized from the measured
// ~4.56 TB/30d window and ~300 GB daily 2-day window, with headroom.
const MAX_BYTES_GB_30D = 6000
const MAX_BYTES_GB_DAILY = 2000

const ROWS_PER_CHUNK = 1_000_000

const STAGING_DDL_30D = `CREATE UNLOGGED TABLE IF NOT EXISTS ${PYPI_DOWNLOADS_30D_STAGING} (
  project   text,
  downloads bigint
)`
const PG_COLUMNS_30D = ['project', 'downloads']

const STAGING_DDL_DAILY = `CREATE UNLOGGED TABLE IF NOT EXISTS ${PYPI_DOWNLOADS_DAILY_STAGING} (
  project   text,
  day       date,
  downloads bigint
)`
const PG_COLUMNS_DAILY = ['project', 'day', 'downloads']

// Shared GCS-parquet → staging → merge driver: chunk the export files so a single staging load
// stays bounded, merging each chunk into the target table(s). Mirrors ingestPackages/ingestDependentCounts.
async function loadAndMerge(params: {
  jobId: number
  gcsPrefix: string
  stagingTable: string
  stagingDdl: string
  pgColumns: string[]
  mergeSql: string | string[]
  tableNames: string | string[]
}): Promise<void> {
  const { fileNames, rowCounts } = await listParquetFiles({ gcsPrefix: params.gcsPrefix })
  const totalFiles = fileNames.length

  if (totalFiles === 0) {
    await mergeStagingToTable({ jobId: params.jobId, mergeSql: [], tableNames: [], isFinal: true })
    return
  }

  const totalRows = rowCounts.reduce((a, b) => a + b, 0)
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
      jobId: params.jobId,
      stagingTable: params.stagingTable,
      stagingDdl: params.stagingDdl,
      pgColumns: params.pgColumns,
      fileNames: chunk,
      filesOffset: start,
      totalFiles,
      priorStagingRows,
    })
    priorStagingRows += rowsLoaded

    const { rowsAffected, tableRowCounts } = await mergeStagingToTable({
      jobId: params.jobId,
      mergeSql: params.mergeSql,
      tableNames: params.tableNames,
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

// Last-30-day downloads for ALL pypi packages. Scheduled monthly with no fromDate → only the
// latest window (mirrored onto packages.downloads_last_30d). Pass fromDate to backfill every
// monthly 30-day bucket from that date up to the current one.
export async function ingestPypiDownloadsLast30d(opts: { fromDate?: string }): Promise<void> {
  const start = workflowInfo().startTime
  const baseRunId = start.toISOString().replace(/[:.]/g, '-')
  const today = start.toISOString().slice(0, 10)
  const upperEndDate = utcFirstOfCurrentMonth(today)

  const windows = computeLast30dWindows(opts.fromDate ?? null, upperEndDate)

  for (const window of windows) {
    const exportResult = await bqExportToGcs({
      jobKind: PYPI_DOWNLOADS_30D_KIND,
      sql: buildPypiDownloads30dSql({ startDate: window.start, endDate: window.end }),
      runId: `${baseRunId}-${window.end}`,
      syncMode: 'full',
      snapshotAt: window.end,
      maxBytesGb: MAX_BYTES_GB_30D,
    })

    await loadAndMerge({
      jobId: exportResult.jobId,
      gcsPrefix: exportResult.gcsPrefix,
      stagingTable: PYPI_DOWNLOADS_30D_STAGING,
      stagingDdl: STAGING_DDL_30D,
      pgColumns: PG_COLUMNS_30D,
      mergeSql: buildPypiDownloads30dMergeSql({
        startDate: window.start,
        endDate: window.end,
        mirrorToPackages: window.isLatest,
      }),
      tableNames: window.isLatest ? ['downloads_last_30d', 'packages'] : 'downloads_last_30d',
    })
  }
}

// Daily downloads for the critical pypi subset. Scheduled daily with no range → the 2-day
// trailing re-scan window. Pass an explicit startDate/endDate to backfill an arbitrary range.
export async function ingestPypiDownloadsDaily(opts: {
  startDate?: string
  endDate?: string
}): Promise<void> {
  const start = workflowInfo().startTime
  const runId = start.toISOString().replace(/[:.]/g, '-')
  const today = start.toISOString().slice(0, 10)

  // A backfill must supply BOTH bounds; a single bound is a mistake, not a partial range —
  // fail loudly rather than silently scanning the default 2-day window.
  if (Boolean(opts.startDate) !== Boolean(opts.endDate)) {
    throw new Error('ingestPypiDownloadsDaily: startDate and endDate must be provided together')
  }
  const range =
    opts.startDate && opts.endDate
      ? { startDate: opts.startDate, endDate: opts.endDate }
      : defaultDailyRange(today)

  const { count } = await getCriticalPypiCount()
  // Nothing to ingest — skip the (full-partition) BQ scan that would merge zero rows.
  if (count === 0) return

  const exportResult = await bqExportToGcs({
    jobKind: PYPI_DOWNLOADS_DAILY_KIND,
    sql: buildPypiDownloadsDailySql({
      startDate: range.startDate,
      endDate: range.endDate,
    }),
    runId,
    syncMode: 'full',
    snapshotAt: range.endDate,
    maxBytesGb: MAX_BYTES_GB_DAILY,
  })

  await loadAndMerge({
    jobId: exportResult.jobId,
    gcsPrefix: exportResult.gcsPrefix,
    stagingTable: PYPI_DOWNLOADS_DAILY_STAGING,
    stagingDdl: STAGING_DDL_DAILY,
    pgColumns: PG_COLUMNS_DAILY,
    mergeSql: buildPypiDownloadsDailyMergeSql(),
    tableNames: 'downloads_daily',
  })
}
