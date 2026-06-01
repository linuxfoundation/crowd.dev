import { Context } from '@temporalio/activity'
import { QueryExecutor, markJobStatus, updateLoadingProgress } from '@crowd/data-access-layer'
import { getServiceChildLogger } from '@crowd/logging'

import { getPackagesDb } from '../../db'
import { bucket } from '../config'
import { streamParquetRowsFromGcs, toParquetDecimal, toTimestampDate } from '../gcsService'
import { buildInsert } from '../sqlUtils'

const log = getServiceChildLogger('gcsParquetToStaging')

const BATCH_SIZE = 1_000
const MAX_CONCURRENT = 4

export interface GcsToStagingInput {
  jobId: number
  stagingTable: string
  stagingDdl?: string
  pgColumns: string[]
  // BQ exports TIMESTAMP columns as INT64 microseconds (BigInt in parquetjs).
  // List any timestamptz columns here so they get converted before INSERT.
  timestampColumns?: string[]
  // BQ NUMERIC columns are exported as 16-byte big-endian buffers (scale=9).
  // List any float/numeric columns backed by BQ NUMERIC so they get converted before INSERT.
  decimalColumns?: string[]
  // If provided, load exactly these GCS object names (skip listing). Used for chunked ingestion.
  fileNames?: string[]
  // If fileNames is not provided, list from this prefix.
  gcsPrefix?: string
  // For progress reporting across chunks: how many files were processed before this batch.
  filesOffset?: number
  // Grand total file count across all chunks. Defaults to this batch's count.
  totalFiles?: number
  // Rows already staged in prior chunks — added to this chunk's count for cumulative row_count_staging.
  priorStagingRows?: number
}

export interface GcsToStagingOutput {
  rowsLoaded: number
}

async function loadParquetFile(
  qx: QueryExecutor,
  table: string,
  columns: string[],
  objectName: string,
  tsCols: Set<string>,
  decCols: Set<string>,
): Promise<number> {
  let batch: Record<string, unknown>[] = []
  let totalLoaded = 0

  for await (const row of streamParquetRowsFromGcs(objectName)) {
    if (tsCols.size > 0) {
      for (const col of tsCols) {
        if (col in row) row[col] = toTimestampDate(row[col])
      }
    }
    if (decCols.size > 0) {
      for (const col of decCols) {
        if (col in row) row[col] = toParquetDecimal(row[col])
      }
    }
    batch.push(row)
    if (batch.length >= BATCH_SIZE) {
      try {
        await qx.result(buildInsert(table, columns, batch))
      } catch (err) {
        log.error({ err, firstRow: batch[0], lastRow: batch[batch.length - 1] }, 'Batch insert failed — dumping first/last row for diagnosis')
        throw err
      }
      totalLoaded += batch.length
      batch = []
    }
  }

  if (batch.length > 0) {
    try {
      await qx.result(buildInsert(table, columns, batch))
    } catch (err) {
      log.error({ err, firstRow: batch[0], lastRow: batch[batch.length - 1] }, 'Final batch insert failed — dumping first/last row for diagnosis')
      throw err
    }
    totalLoaded += batch.length
  }

  return totalLoaded
}

function gcsPrefixToObjectPrefix(gcsPrefix: string): string {
  // gs://bucket-name/path/to/prefix/ → path/to/prefix/
  const withoutScheme = gcsPrefix.replace(/^gs:\/\//, '')
  const slashIdx = withoutScheme.indexOf('/')
  return slashIdx >= 0 ? withoutScheme.slice(slashIdx + 1) : ''
}

export async function gcsParquetToStaging(
  input: GcsToStagingInput,
): Promise<GcsToStagingOutput> {
  const { jobId, stagingTable, stagingDdl, pgColumns, timestampColumns, decimalColumns } = input
  const tsCols = new Set(timestampColumns ?? [])
  const decCols = new Set(decimalColumns ?? [])
  const filesOffset = input.filesOffset ?? 0

  const qx = await getPackagesDb()

  await markJobStatus(qx, jobId, 'loading')
  if (stagingDdl) await qx.result(stagingDdl)
  await qx.result(`TRUNCATE ${stagingTable}`)
  // Reset progress at the start of a fresh run (first chunk). This handles job ID reuse
  // via --export-name: a previous run's 193/193 would otherwise persist due to GREATEST.
  if (filesOffset === 0 && input.totalFiles != null) {
    await updateLoadingProgress(qx, jobId, 0, input.totalFiles, true)
  }

  let parquetFileNames: string[]
  if (input.fileNames) {
    parquetFileNames = input.fileNames
  } else if (input.gcsPrefix) {
    const objectPrefix = gcsPrefixToObjectPrefix(input.gcsPrefix)
    const [files] = await bucket.getFiles({ prefix: objectPrefix })
    parquetFileNames = files.filter((f) => f.name.endsWith('.parquet')).map((f) => f.name)
  } else {
    throw new Error('gcsParquetToStaging: must provide either fileNames or gcsPrefix')
  }

  const totalFiles = input.totalFiles ?? parquetFileNames.length

  log.info({ jobId, stagingTable, fileCount: parquetFileNames.length, filesOffset, totalFiles }, 'Loading parquet files into staging')

  let totalLoaded = 0
  let chunkIdx = 0

  for (let i = 0; i < parquetFileNames.length; i += MAX_CONCURRENT) {
    const chunk = parquetFileNames.slice(i, i + MAX_CONCURRENT)
    const counts = await Promise.all(
      chunk.map((name) => loadParquetFile(qx, stagingTable, pgColumns, name, tsCols, decCols)),
    )
    totalLoaded += counts.reduce((a, b) => a + b, 0)
    const doneInBatch = i + chunk.length
    const doneGlobal = filesOffset + doneInBatch
    log.info(
      {
        jobId,
        totalLoaded,
        progress: `${doneGlobal}/${totalFiles} (${Math.round((doneGlobal / totalFiles) * 100)}%)`,
      },
      'Staging load progress',
    )
    Context.current().heartbeat({ done: doneGlobal, total: totalFiles })
    if (chunkIdx % 10 === 0 || doneInBatch === parquetFileNames.length) {
      await updateLoadingProgress(qx, jobId, doneGlobal, totalFiles)
    }
    chunkIdx++
  }

  const cumulativeStagingRows = (input.priorStagingRows ?? 0) + totalLoaded
  await markJobStatus(qx, jobId, 'loading', {
    rowCountStaging: cumulativeStagingRows,
    tableRowCounts: { [`staging:${stagingTable}`]: totalLoaded },
  })

  log.info({ jobId, stagingTable, totalLoaded }, 'Staging load complete')

  return { rowsLoaded: totalLoaded }
}
