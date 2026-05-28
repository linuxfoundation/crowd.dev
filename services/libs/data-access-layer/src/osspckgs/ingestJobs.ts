import { QueryExecutor } from '../queryExecutor'

export type OsspckgsJobKind =
  | 'packages'
  | 'versions'
  | 'package_dependencies'
  | 'repos'
  | 'package_repos'
  | 'advisories'
  | 'advisory_packages'

export type OsspckgsJobStatus =
  | 'pending'
  | 'exporting'
  | 'exported'
  | 'loading'
  | 'merging'
  | 'done'
  | 'failed'
  | 'cleaned'

export type OsspckgsSyncMode = 'full' | 'incremental'

export interface MarkJobStatusFields {
  gcsPrefix?: string
  rowCountBq?: number
  rowCountPg?: number
  bqBytesBilled?: number
  errorMessage?: string
  finishedAt?: Date
  cleanedAt?: Date
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

// Inserts a new job row with status 'pending', returns the new id.
// provisionalSnapshotAt is the expected snapshot date; promoted to snapshot_at
// only on successful non-zero merge (B10 — prevents watermark advancing on empty exports).
export async function createIngestJob(
  qx: QueryExecutor,
  jobKind: OsspckgsJobKind,
  syncMode: OsspckgsSyncMode,
  provisionalSnapshotAt: Date | null,
): Promise<number> {
  const row = await qx.selectOne(
    `
    INSERT INTO osspckgs_ingest_jobs (job_kind, status, sync_mode, provisional_snapshot_at)
    VALUES ($(jobKind), 'pending', $(syncMode), $(provisionalSnapshotAt))
    RETURNING id
    `,
    { jobKind, syncMode, provisionalSnapshotAt },
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
  if (fields.rowCountPg !== undefined) {
    sets.push('row_count_pg = $(rowCountPg)')
    params.rowCountPg = fields.rowCountPg
  }
  if (fields.bqBytesBilled !== undefined) {
    sets.push('bq_bytes_billed = $(bqBytesBilled)')
    params.bqBytesBilled = fields.bqBytesBilled
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

  // B10: promote provisional_snapshot_at → snapshot_at only when done with rows
  if (status === 'done' && fields.rowCountPg !== undefined && fields.rowCountPg > 0) {
    sets.push('snapshot_at = provisional_snapshot_at')
  }

  await qx.result(
    `UPDATE osspckgs_ingest_jobs SET ${sets.join(', ')} WHERE id = $(jobId)`,
    params,
  )
}
