import { getServiceChildLogger } from '@crowd/logging'

import { bucket } from '../config'
import { readParquetRowCount } from '../gcsService'

const log = getServiceChildLogger('listParquetFiles')

export interface ListParquetFilesInput {
  gcsPrefix: string
}

export interface ListParquetFilesOutput {
  fileNames: string[]
  fileSizes: number[]
  rowCounts: number[]
}

export async function listParquetFiles(input: ListParquetFilesInput): Promise<ListParquetFilesOutput> {
  const withoutScheme = input.gcsPrefix.replace(/^gs:\/\//, '')
  const slashIdx = withoutScheme.indexOf('/')
  const objectPrefix = slashIdx >= 0 ? withoutScheme.slice(slashIdx + 1) : ''
  const [files] = await bucket.getFiles({ prefix: objectPrefix })
  const parquet = files.filter((f) => f.name.endsWith('.parquet'))
  const fileNames = parquet.map((f) => f.name)
  const fileSizes = parquet.map((f) => parseInt(f.metadata.size as string ?? '0', 10))
  const rowCounts = await Promise.all(
    parquet.map((f, i) => readParquetRowCount(f.name, fileSizes[i])),
  )
  const totalBytes = fileSizes.reduce((a, b) => a + b, 0)
  const totalRows = rowCounts.reduce((a, b) => a + b, 0)
  log.info({ gcsPrefix: input.gcsPrefix, fileCount: fileNames.length, totalBytes, totalRows }, 'Listed parquet files')
  return { fileNames, fileSizes, rowCounts }
}
