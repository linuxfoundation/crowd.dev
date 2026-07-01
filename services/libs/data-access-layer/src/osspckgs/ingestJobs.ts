import { QueryExecutor } from '../queryExecutor'

export type OsspckgsJobKind =
  | 'packages'
  | 'versions'
  | 'package_dependencies'
  | 'repos'
  | 'package_repos'
  | 'advisories'
  | 'advisory_packages'
  | 'dependent_counts'
  | 'scorecard_repos'
  | 'scorecard_checks'
  | 'ranking'
  | 'pypi_downloads_30d'
  | 'pypi_downloads_daily'

export type OsspckgsJobStatus =
  | 'pending'
  | 'exporting'
  | 'exported'
  | 'loading'
  | 'merging'
  | 'done'
  | 'failed'
  | 'cleaned'

export type OsspckgsSyncMode = 'full' | 'incremental' | 'ranking'

export interface BqStats {
  jobId: string
  cacheHit: boolean
  totalBytesProcessed: number
  totalBytesBilled: number
  totalSlotMs: number
  outputRows?: number
  durationMs?: number
  referencedTables: string[]
  creationTime?: number
  startTime?: number
  endTime?: number
}

export interface MarkJobStatusFields {
  gcsPrefix?: string
  rowCountBq?: number
  rowCountStaging?: number
  rowCountPg?: number
  tableRowCounts?: Record<string, number | string | string[]>
  bqBytesBilled?: number
  bqJobId?: string
  bqStats?: BqStats
  errorMessage?: string
  finishedAt?: Date
  cleanedAt?: Date
  exportName?: string
}

// Returns the most recent job for the given kind that has already been exported to GCS,
// so callers can skip re-running BQ when the user explicitly opts to reuse prior data.
export async function findLatestExportedJobByKind(
  qx: QueryExecutor,
  jobKind: OsspckgsJobKind,
): Promise<{ id: number; gcsPrefix: string; rowCountBq: number; bqBytesBilled: number } | null> {
  const row = await qx.selectOneOrNone(
    `
    SELECT id, gcs_prefix, row_count_bq, bq_bytes_billed
    FROM osspckgs_ingest_jobs
    WHERE job_kind = $(jobKind)
      AND gcs_prefix IS NOT NULL
      AND status IN ('exported', 'loading', 'merging', 'done', 'cleaned')
    ORDER BY id DESC
    LIMIT 1
    `,
    { jobKind },
  )
  return row
    ? {
        id: Number(row.id),
        gcsPrefix: row.gcs_prefix as string,
        rowCountBq: Number(row.row_count_bq ?? 0),
        bqBytesBilled: Number(row.bq_bytes_billed ?? 0),
      }
    : null
}

// Returns an existing exported job for the given GCS prefix so callers can
// skip re-running the BQ export when retrying a failed workflow.
export async function findExportedJobByGcsPrefix(
  qx: QueryExecutor,
  gcsPrefix: string,
): Promise<{ id: number; rowCountBq: number; bqBytesBilled: number } | null> {
  const row = await qx.selectOneOrNone(
    `
    SELECT id, row_count_bq, bq_bytes_billed
    FROM osspckgs_ingest_jobs
    WHERE gcs_prefix = $(gcsPrefix)
      AND status IN ('exported', 'loading', 'merging', 'done', 'cleaned')
    ORDER BY id DESC
    LIMIT 1
    `,
    { gcsPrefix },
  )
  return row
    ? {
        id: Number(row.id),
        rowCountBq: Number(row.row_count_bq ?? 0),
        bqBytesBilled: Number(row.bq_bytes_billed ?? 0),
      }
    : null
}

// Returns the SnapshotAt date of the last successful run for this job_kind.
// Returns null if no successful run exists (bootstrap required).
export async function getLastSuccessfulSnapshot(
  qx: QueryExecutor,
  jobKind: OsspckgsJobKind,
): Promise<Date | null> {
  const row = await qx.selectOneOrNone(
    `
    SELECT snapshot_at
    FROM osspckgs_ingest_jobs
    WHERE job_kind = $(jobKind)
      AND status = 'done'
      AND snapshot_at IS NOT NULL
    ORDER BY snapshot_at DESC
    LIMIT 1
    `,
    { jobKind },
  )
  return row ? new Date(row.snapshot_at) : null
}

// Returns an existing named export for the given (job_kind, export_name) pair.
// Used by bqExportToGcs when --export-name is passed to skip BQ and load from GCS.
export async function findExportByKindAndName(
  qx: QueryExecutor,
  jobKind: OsspckgsJobKind,
  exportName: string,
): Promise<{ id: number; gcsPrefix: string; rowCountBq: number; bqBytesBilled: number } | null> {
  const row = await qx.selectOneOrNone(
    `
    SELECT id, gcs_prefix, row_count_bq, bq_bytes_billed
    FROM osspckgs_ingest_jobs
    WHERE job_kind   = $(jobKind)
      AND export_name = $(exportName)
      AND gcs_prefix IS NOT NULL
      AND status IN ('exported', 'loading', 'merging', 'done', 'cleaned')
    ORDER BY id DESC
    LIMIT 1
    `,
    { jobKind, exportName },
  )
  return row
    ? {
        id: Number(row.id),
        gcsPrefix: row.gcs_prefix as string,
        rowCountBq: Number(row.row_count_bq ?? 0),
        bqBytesBilled: Number(row.bq_bytes_billed ?? 0),
      }
    : null
}

// Writes file-level loading progress into table_row_counts so the monitor can
// display x/y (z%) while gcsParquetToStaging is running. Uses JSONB merge so
// other keys (staging:, bq:) are preserved.
//
// reset=true: plain overwrite — used by the first chunk of a new run (filesOffset=0)
//   so a re-run on the same job ID (via --export-name reuse) starts from 0 again.
// reset=false (default): GREATEST — guards against a retried activity writing a
//   lower value than a previous attempt of the same chunk already committed.
export async function updateLoadingProgress(
  qx: QueryExecutor,
  jobId: number,
  done: number,
  total: number,
  reset = false,
): Promise<void> {
  const doneSql = reset
    ? `$(done)::int`
    : `GREATEST(COALESCE((table_row_counts->>'progress:done')::int, 0), $(done)::int)`
  await qx.result(
    `UPDATE osspckgs_ingest_jobs
     SET table_row_counts = COALESCE(table_row_counts, '{}'::jsonb)
       || jsonb_build_object('progress:done', ${doneSql}, 'progress:total', $(total)::int)
     WHERE id = $(jobId)`,
    { jobId, done, total },
  )
}

// Returns the MAX row_count_bq across the last 5 completed jobs for this kind.
// Using MAX over recent jobs rather than the latest single job means one anomalous
// run (e.g. a bad upstream snapshot) cannot corrupt the baseline for the next guard check.
export async function getLastCompletedJobRowCount(
  qx: QueryExecutor,
  jobKind: OsspckgsJobKind,
): Promise<number | null> {
  const row = await qx.selectOneOrNone(
    `
    SELECT MAX(row_count_bq) AS row_count_bq
    FROM (
      SELECT row_count_bq
      FROM osspckgs_ingest_jobs
      WHERE job_kind = $(jobKind)
        AND status IN ('done', 'cleaned')
        AND row_count_bq IS NOT NULL
      ORDER BY id DESC
      LIMIT 5
    ) recent
    `,
    { jobKind },
  )
  return row?.row_count_bq != null ? Number(row.row_count_bq) : null
}

// Inserts a new job row with status 'pending', returns the new id.
// provisionalSnapshotAt is the expected snapshot date; promoted to snapshot_at
// unconditionally when the job reaches 'done' — including quiet incremental windows
// with 0 rows, so the watermark always advances and the same date is never re-queried.
export async function createIngestJob(
  qx: QueryExecutor,
  jobKind: OsspckgsJobKind,
  syncMode: OsspckgsSyncMode,
  provisionalSnapshotAt: Date | null,
  exportName?: string,
): Promise<number> {
  const row = await qx.selectOne(
    `
    INSERT INTO osspckgs_ingest_jobs (job_kind, status, sync_mode, provisional_snapshot_at, export_name)
    VALUES ($(jobKind), 'pending', $(syncMode), $(provisionalSnapshotAt), $(exportName))
    RETURNING id
    `,
    { jobKind, syncMode, provisionalSnapshotAt, exportName: exportName ?? null },
  )
  return row.id
}

// Updates status and optional fields on an existing job row.
export async function markJobStatus(
  qx: QueryExecutor,
  jobId: number,
  status: OsspckgsJobStatus,
  fields: MarkJobStatusFields = {},
): Promise<void> {
  const sets: string[] = ['status = $(status)']
  const params: Record<string, unknown> = { jobId, status }

  if (fields.gcsPrefix !== undefined) {
    sets.push('gcs_prefix = $(gcsPrefix)')
    params.gcsPrefix = fields.gcsPrefix
  }
  if (fields.rowCountBq !== undefined) {
    sets.push('row_count_bq = $(rowCountBq)')
    params.rowCountBq = fields.rowCountBq
  }
  if (fields.rowCountStaging !== undefined) {
    sets.push('row_count_staging = $(rowCountStaging)')
    params.rowCountStaging = fields.rowCountStaging
  }
  if (fields.rowCountPg !== undefined) {
    sets.push('row_count_pg = $(rowCountPg)')
    params.rowCountPg = fields.rowCountPg
  }
  if (fields.tableRowCounts !== undefined) {
    // Merge into existing jsonb so each pipeline stage accumulates its own keys.
    sets.push("table_row_counts = COALESCE(table_row_counts, '{}'::jsonb) || $(tableRowCounts)")
    params.tableRowCounts = fields.tableRowCounts
  }
  if (fields.bqBytesBilled !== undefined) {
    sets.push('bq_bytes_billed = $(bqBytesBilled)')
    params.bqBytesBilled = fields.bqBytesBilled
  }
  if (fields.bqJobId !== undefined) {
    sets.push('bq_job_id = $(bqJobId)')
    params.bqJobId = fields.bqJobId
  }
  if (fields.bqStats !== undefined) {
    sets.push('bq_stats = $(bqStats)')
    params.bqStats = fields.bqStats
  }
  if (fields.errorMessage !== undefined) {
    sets.push('error_message = $(errorMessage)')
    params.errorMessage = fields.errorMessage
  }
  if (fields.finishedAt !== undefined) {
    sets.push('finished_at = $(finishedAt)')
    params.finishedAt = fields.finishedAt
  }
  if (fields.cleanedAt !== undefined) {
    sets.push('cleaned_at = $(cleanedAt)')
    params.cleanedAt = fields.cleanedAt
  }
  if (fields.exportName !== undefined) {
    sets.push('export_name = $(exportName)')
    params.exportName = fields.exportName
  }

  // B10: promote provisional_snapshot_at → snapshot_at on completion — unconditional so that
  // incremental runs with 0 new rows still advance the watermark. A quiet window is a valid
  // outcome and must not cause the next run to re-query the same date range indefinitely.
  if (status === 'done') {
    sets.push('snapshot_at = provisional_snapshot_at')
  }

  await qx.result(`UPDATE osspckgs_ingest_jobs SET ${sets.join(', ')} WHERE id = $(jobId)`, params)
}

// Merges key-value pairs into table_row_counts without changing status.
// Used to write meta:step and similar display-only tracking keys mid-job.
export async function mergeJobTableRowCounts(
  qx: QueryExecutor,
  jobId: number,
  kv: Record<string, number | string | string[]>,
): Promise<void> {
  await qx.result(
    `UPDATE osspckgs_ingest_jobs
     SET table_row_counts = COALESCE(table_row_counts, '{}'::jsonb) || $(kv)::jsonb
     WHERE id = $(jobId)`,
    { jobId, kv },
  )
}
