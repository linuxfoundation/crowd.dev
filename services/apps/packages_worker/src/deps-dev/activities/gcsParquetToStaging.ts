import { QueryExecutor, markJobStatus } from '@crowd/data-access-layer'
import { getServiceChildLogger } from '@crowd/logging'

import { getPackagesDb } from '../../db'
import { bucket } from '../config'
import { streamParquetRowsFromGcs } from '../gcsService'
import { buildInsert } from '../sqlUtils'

const log = getServiceChildLogger('gcsParquetToStaging')

const BATCH_SIZE = 1_000
const MAX_CONCURRENT = 4

export interface GcsToStagingInput {
  jobId: number
  gcsPrefix: string
  stagingTable: string
  stagingDdl?: string
  pgColumns: string[]
}

export interface GcsToStagingOutput {
  rowsLoaded: number
}

async function loadParquetFile(
  qx: QueryExecutor,
  table: string,
  columns: string[],
  objectName: string,
): Promise<number> {
  let batch: Record<string, unknown>[] = []
  let totalLoaded = 0

  for await (const row of streamParquetRowsFromGcs(objectName)) {
    batch.push(row)
    if (batch.length >= BATCH_SIZE) {
      await qx.result(buildInsert(table, columns, batch))
      totalLoaded += batch.length
      batch = []
    }
  }

  if (batch.length > 0) {
    await qx.result(buildInsert(table, columns, batch))
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
  const { jobId, gcsPrefix, stagingTable, stagingDdl, pgColumns } = input

  const qx = await getPackagesDb()

  await markJobStatus(qx, jobId, 'loading')
  if (stagingDdl) await qx.result(stagingDdl)
  await qx.result(`TRUNCATE ${stagingTable}`)

  const objectPrefix = gcsPrefixToObjectPrefix(gcsPrefix)
  const [files] = await bucket.getFiles({ prefix: objectPrefix })
  const parquetFiles = files.filter((f) => f.name.endsWith('.parquet'))

  log.info({ jobId, stagingTable, fileCount: parquetFiles.length }, 'Loading parquet files into staging')

  let totalLoaded = 0

  for (let i = 0; i < parquetFiles.length; i += MAX_CONCURRENT) {
    const chunk = parquetFiles.slice(i, i + MAX_CONCURRENT)
    const counts = await Promise.all(
      chunk.map((f) => loadParquetFile(qx, stagingTable, pgColumns, f.name)),
    )
    totalLoaded += counts.reduce((a, b) => a + b, 0)
    log.info({ jobId, totalLoaded, progress: `${i + chunk.length}/${parquetFiles.length}` }, 'Staging load progress')
  }

  await markJobStatus(qx, jobId, 'loading', { rowCountPg: totalLoaded })

  log.info({ jobId, stagingTable, totalLoaded }, 'Staging load complete')

  return { rowsLoaded: totalLoaded }
}
