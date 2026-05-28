import {
  OsspckgsJobKind,
  OsspckgsSyncMode,
  createIngestJob,
  markJobStatus,
} from '@crowd/data-access-layer'
import { getServiceChildLogger } from '@crowd/logging'

import { getPackagesDb } from '../../db'
import { GCS_BUCKET, bigquery } from '../config'

const log = getServiceChildLogger('bqExportToGcs')

export interface BqExportToGcsInput {
  jobKind: OsspckgsJobKind
  sql: string
  runId: string
  syncMode: OsspckgsSyncMode
  snapshotAt: string | null
  maxBytesGb: number
}

export interface BqExportToGcsOutput {
  gcsPrefix: string
  rowCount: number
  bqBytesBilled: number
  jobId: number
}

export async function bqExportToGcs(input: BqExportToGcsInput): Promise<BqExportToGcsOutput> {
  const { jobKind, sql, runId, syncMode, snapshotAt, maxBytesGb } = input

  // M4: explicit location to avoid cross-region error when account default != US
  const [dryRunJob] = await bigquery.createQueryJob({ query: sql, dryRun: true, location: 'US' })
  const dryRunBytes = Number(dryRunJob.metadata.statistics.totalBytesProcessed ?? 0)
  log.info({ jobKind, dryRunBytes, maxBytesGb }, 'BQ dry-run complete')

  const ceiling = maxBytesGb * 1e9
  if (dryRunBytes > ceiling) {
    throw new Error(
      `BQ dry-run for ${jobKind} reports ${dryRunBytes} bytes > ceiling ${ceiling} — aborting`,
    )
  }

  const qx = await getPackagesDb()
  const provisionalDate = snapshotAt ? new Date(snapshotAt) : null
  const jobId = await createIngestJob(qx, jobKind, syncMode, provisionalDate)

  // H7: mark exporting before we start the BQ job
  await markJobStatus(qx, jobId, 'exporting')

  const gcsPrefix = `gs://${GCS_BUCKET}/osspckgs/${jobKind}/${runId}/`
  // B9: wrap in SELECT * FROM (...) so QUALIFY / top-level set ops don't break EXPORT DATA syntax
  const exportSql = `EXPORT DATA OPTIONS(
  uri='${gcsPrefix}*.parquet',
  format='PARQUET',
  compression='SNAPPY',
  overwrite=true
) AS SELECT * FROM (${sql.replace(/;\s*$/, '')})`

  log.info({ jobKind, jobId, gcsPrefix }, 'Starting BQ export')

  const [job] = await bigquery.createQueryJob({ query: exportSql, location: 'US' })
  await job.promise()
  const [metadata] = await job.getMetadata()

  const rowCount = Number(metadata.statistics?.query?.outputRows ?? 0)
  const bqBytesBilled = Number(metadata.statistics?.totalBytesProcessed ?? 0)

  await markJobStatus(qx, jobId, 'exported', {
    gcsPrefix,
    rowCountBq: rowCount,
    bqBytesBilled,
  })

  log.info({ jobKind, jobId, rowCount, bqBytesBilled }, 'BQ export complete')

  return { gcsPrefix, rowCount, bqBytesBilled, jobId }
}
