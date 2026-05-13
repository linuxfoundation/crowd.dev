/**
 * Snowflake COPY INTO logic.
 *
 * Responsible for executing COPY INTO queries against Snowflake
 * to export data into S3 as Parquet files.
 */
import { getServiceChildLogger } from '@crowd/logging'

import { SnowflakeClient } from './client'

const log = getServiceChildLogger('snowflakeExporter')

const DEFAULT_BATCH_SIZE = 10_000

export type OnBatchComplete = (
  s3Path: string,
  totalRows: number,
  totalBytes: number,
) => Promise<void>

interface CopyIntoRow {
  rows_unloaded: number
  input_bytes: number
  output_bytes: number
  fileName: string
}

/**
 * Build the S3 filename prefix for a batched COPY INTO export.
 * Format: {CROWD_SNOWFLAKE_S3_BUCKET_PATH}/{platform}/{sourceName}/{yyyy}/{mm}/{dd}
 */
export function buildS3FilenamePrefix(platform: string, sourceName: string): string {
  const s3BucketPath = process.env.CROWD_SNOWFLAKE_S3_BUCKET_PATH
  if (!s3BucketPath) {
    throw new Error('Missing required env var CROWD_SNOWFLAKE_S3_BUCKET_PATH')
  }
  const now = new Date()
  const year = now.getFullYear()
  const month = String(now.getMonth() + 1).padStart(2, '0')
  const day = String(now.getDate()).padStart(2, '0')
  return `${s3BucketPath}/${platform}/${sourceName}/${year}/${month}/${day}`
}

export class SnowflakeExporter {
  private readonly snowflake: SnowflakeClient

  constructor() {
    this.snowflake = SnowflakeClient.fromEnv({ parentLog: log })
  }

  async destroy(): Promise<void> {
    await this.snowflake.destroy()
  }

  async executeBatchedCopyInto(
    sourceQuery: string,
    s3FilenamePrefix: string,
    onBatchComplete?: OnBatchComplete,
  ): Promise<void> {
    const storageIntegration = process.env.CROWD_SNOWFLAKE_STORAGE_INTEGRATION
    if (!storageIntegration) {
      throw new Error('Missing required env var CROWD_SNOWFLAKE_STORAGE_INTEGRATION')
    }
    const limit = DEFAULT_BATCH_SIZE
    let offset = 0
    let batch = 0
    let hasMoreRows = true

    while (hasMoreRows) {
      const filename = `batch_${batch}.parquet`
      const s3Path = `${s3FilenamePrefix}/${filename}`

      // TODO: LIMIT/OFFSET over mutable data can skip or duplicate rows if the source changes between batches.
      // Consider materializing a temp table from the source query first, then paginating over it.
      const copyQuery = `
        COPY INTO '${s3Path}'
        FROM (${sourceQuery} LIMIT ${limit} OFFSET ${offset})
        STORAGE_INTEGRATION = "${storageIntegration}"
        FILE_FORMAT = (TYPE = PARQUET)
        HEADER = TRUE
        SINGLE = TRUE
        OVERWRITE = TRUE
      `

      log.info(
        { s3FilenamePrefix, storageIntegration, batch, offset, limit },
        'Executing COPY INTO batch',
      )

      const results = await this.snowflake.run<CopyIntoRow>(copyQuery)

      if (results.length === 0) {
        log.info({ batch, totalRowsExported: offset }, 'No more rows to export')
        hasMoreRows = false
        continue
      }

      const batchRows = results.reduce((sum, r) => sum + r.rows_unloaded, 0)
      const batchBytes = results.reduce((sum, r) => sum + r.output_bytes, 0)

      log.info({ batch, batchRows, batchBytes, files: results.length }, 'COPY INTO batch completed')

      if (onBatchComplete && batchRows > 0) {
        await onBatchComplete(s3Path, batchRows, batchBytes)
      }
      if (batchRows < limit) {
        log.info(
          { totalRowsExported: offset + batchRows },
          'Export finished (last batch was partial)',
        )
        hasMoreRows = false
        continue
      }

      offset += limit
      batch++
    }
  }
}
