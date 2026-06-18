import {
  OsspckgsJobKind,
  OsspckgsSyncMode,
  createIngestJob,
  findExportByKindAndName,
  findExportedJobByGcsPrefix,
  findLatestExportedJobByKind,
  markJobStatus,
} from '@crowd/data-access-layer'
import { getServiceChildLogger } from '@crowd/logging'

import { getPackagesDb } from '../../db'
import { extractBqStats } from '../bqStats'
import { GCS_BUCKET, bigquery, bucket } from '../config'

const log = getServiceChildLogger('bqExportToGcs')

export interface BqExportToGcsInput {
  jobKind: OsspckgsJobKind
  sql: string
  runId: string
  syncMode: OsspckgsSyncMode
  snapshotAt: string | null
  maxBytesGb: number
  reuseExports?: boolean
  exportName?: string
}

export interface BqExportToGcsOutput {
  gcsPrefix: string
  rowCount: number
  bqBytesBilled: number
  jobId: number
}

export async function bqExportToGcs(input: BqExportToGcsInput): Promise<BqExportToGcsOutput> {
  const { jobKind, sql, runId, syncMode, snapshotAt, maxBytesGb, reuseExports, exportName } = input

  // Named exports use a stable GCS path independent of runId so they survive across bootstrap runs.
  const namedGcsPrefix = exportName
    ? `gs://${GCS_BUCKET}/osspckgs/${jobKind}/exports/${exportName}/`
    : null
  const namedFolderPath = exportName ? `osspckgs/${jobKind}/exports/${exportName}/` : null

  const gcsPrefix = namedGcsPrefix ?? `gs://${GCS_BUCKET}/osspckgs/${jobKind}/${runId}/`
  const gcsFolderPath = namedFolderPath ?? `osspckgs/${jobKind}/${runId}/`

  const qx = await getPackagesDb()

  // Named export: look up by (job_kind, export_name) — most explicit reuse mode.
  if (exportName) {
    const namedPrefix: string = namedGcsPrefix ?? ''
    const namedFolder: string = namedFolderPath ?? ''
    const prior = await findExportByKindAndName(qx, jobKind, exportName)
    if (prior) {
      const priorFolderPath = prior.gcsPrefix.replace(`gs://${GCS_BUCKET}/`, '')
      const [priorFiles] = await bucket.getFiles({ prefix: priorFolderPath, maxResults: 1 })
      if (priorFiles.length > 0) {
        log.info(
          { jobKind, exportName, jobId: prior.id, gcsPrefix: prior.gcsPrefix },
          'exportName match — skipping BQ, loading from named export',
        )
        return {
          gcsPrefix: prior.gcsPrefix,
          rowCount: prior.rowCountBq,
          bqBytesBilled: 0,
          jobId: prior.id,
        }
      }
      log.warn(
        { jobKind, exportName, jobId: prior.id },
        'named export found in DB but GCS files gone (expired?), falling through to BQ',
      )
    } else {
      // DB empty (e.g. scaffold reset) — check GCS directly before hitting BQ.
      const [existingNamedFiles] = await bucket.getFiles({
        prefix: namedFolder,
        maxResults: 1,
      })
      if (existingNamedFiles.length > 0) {
        log.info(
          { jobKind, exportName, gcsPrefix: namedPrefix },
          'no DB record but GCS files exist — re-registering named export (scaffold reset?)',
        )
        const jobId = await createIngestJob(
          qx,
          jobKind,
          syncMode,
          snapshotAt ? new Date(snapshotAt) : null,
          exportName,
        )
        await markJobStatus(qx, jobId, 'exported', {
          gcsPrefix: namedPrefix,
          rowCountBq: 0,
          bqBytesBilled: 0,
          tableRowCounts: { 'bq:export': 0 },
        })
        return { gcsPrefix: namedPrefix, rowCount: 0, bqBytesBilled: 0, jobId }
      }
      log.info({ jobKind, exportName }, 'no named export in DB or GCS — running BQ export')
    }
  }

  // Implicit reuse: skip BQ entirely and load from any prior exported run.
  // Verifies files still exist in GCS before trusting DB metadata (lifecycle rules may have deleted them).
  if (reuseExports && !exportName) {
    const prior = await findLatestExportedJobByKind(qx, jobKind)
    if (prior) {
      const priorFolderPath = prior.gcsPrefix.replace(`gs://${GCS_BUCKET}/`, '')
      const [priorFiles] = await bucket.getFiles({ prefix: priorFolderPath, maxResults: 1 })
      if (priorFiles.length > 0) {
        log.info(
          { jobKind, jobId: prior.id, gcsPrefix: prior.gcsPrefix },
          'reuseExports=true — skipping BQ, loading from prior export',
        )
        return {
          gcsPrefix: prior.gcsPrefix,
          rowCount: prior.rowCountBq,
          bqBytesBilled: 0,
          jobId: prior.id,
        }
      }
      log.warn(
        { jobKind, jobId: prior.id, gcsPrefix: prior.gcsPrefix },
        'reuseExports=true — prior export found in DB but GCS files are gone (expired?), falling through to BQ',
      )
    } else {
      log.warn(
        { jobKind },
        'reuseExports=true but no prior export found in DB — falling through to BQ',
      )
    }
  }

  // Reuse a previous export for the same runId (or same named export path) — avoids re-billing BQ on Temporal retries.
  const [existingFiles] = await bucket.getFiles({ prefix: gcsFolderPath, maxResults: 1 })
  if (existingFiles.length > 0) {
    const existing = await findExportedJobByGcsPrefix(qx, gcsPrefix)
    if (existing) {
      log.info(
        { jobKind, jobId: existing.id, gcsPrefix },
        'GCS files already exist — reusing export',
      )
      return { gcsPrefix, rowCount: existing.rowCountBq, bqBytesBilled: 0, jobId: existing.id }
    }
  }

  // M4: explicit location to avoid cross-region error when account default != US
  const [dryRunJob] = await bigquery.createQueryJob({ query: sql, dryRun: true, location: 'US' })
  const dryRunBytes = Number(dryRunJob.metadata.statistics.totalBytesProcessed ?? 0)
  log.info({ jobKind, dryRunBytes, maxBytesGb }, 'BQ dry-run complete')

  // Override table is in src/deps-dev/README.md — update it when adding new job kinds.
  // Mode-specific key takes precedence over the generic key (needed for kinds like "packages"
  // that have separate full/incremental ceilings: BQ_DATASET_INGEST_PACKAGES_FULL_MAX_BQ_GB).
  const baseKey = `BQ_DATASET_INGEST_${jobKind.toUpperCase().replace(/-/g, '_')}`
  const modeKey = `${baseKey}_${syncMode.toUpperCase()}_MAX_BQ_GB`
  const genericKey = `${baseKey}_MAX_BQ_GB`
  const activeKey = process.env[modeKey] !== undefined ? modeKey : genericKey
  const envOverride = process.env[activeKey]
  if (envOverride !== undefined) {
    const parsed = Number(envOverride)
    if (!isFinite(parsed) || parsed <= 0) {
      throw new Error(
        `Invalid env ${activeKey}="${envOverride}" — must be a positive finite number`,
      )
    }
  }
  const effectiveMaxBytesGb = envOverride !== undefined ? Number(envOverride) : maxBytesGb
  const ceiling = effectiveMaxBytesGb * 1e9
  if (dryRunBytes > ceiling) {
    throw new Error(
      `BQ dry-run for ${jobKind} reports ${dryRunBytes} bytes > ceiling ${ceiling} — aborting`,
    )
  }

  const provisionalDate = snapshotAt ? new Date(snapshotAt) : null
  const jobId = await createIngestJob(qx, jobKind, syncMode, provisionalDate, exportName)

  // H7: mark exporting before we start the BQ job
  await markJobStatus(qx, jobId, 'exporting')

  // B9: wrap in SELECT * FROM (...) so QUALIFY / top-level set ops don't break EXPORT DATA syntax.
  // CREATE TEMP TABLE first so BQ materializes the result before exporting — direct EXPORT DATA
  // from a subquery produces O(rows) micro-files (~1 KB each); a temp table forces proper sharding.
  const innerSql = sql.replace(/;\s*$/, '')
  const exportSql = `
CREATE TEMP TABLE _export_data AS SELECT * FROM (${innerSql});
EXPORT DATA OPTIONS(
  uri='${gcsPrefix}*.parquet',
  format='PARQUET',
  compression='SNAPPY',
  overwrite=true
) AS SELECT * FROM _export_data;
`

  log.info({ jobKind, jobId, gcsPrefix }, 'Starting BQ export')

  const [job] = await bigquery.createQueryJob({ query: exportSql, location: 'US' })
  await job.promise()
  const bqStats = await extractBqStats(job, bigquery)

  const rowCount = bqStats.outputRows ?? 0

  await markJobStatus(qx, jobId, 'exported', {
    gcsPrefix,
    rowCountBq: rowCount,
    bqBytesBilled: bqStats.bqBytesBilled,
    bqJobId: bqStats.bqJobId,
    bqStats,
    tableRowCounts: { 'bq:export': rowCount },
  })

  log.info(
    {
      jobKind,
      jobId,
      rowCount,
      bqJobId: bqStats.bqJobId,
      totalBytesProcessed: bqStats.totalBytesProcessed,
      totalSlotMs: bqStats.totalSlotMs,
      durationMs: bqStats.durationMs,
    },
    'BQ export complete',
  )

  return { gcsPrefix, rowCount, bqBytesBilled: bqStats.bqBytesBilled, jobId }
}
