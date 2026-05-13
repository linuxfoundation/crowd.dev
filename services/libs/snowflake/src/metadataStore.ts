/**
 * Database operations for integration.snowflakeExportJobs.
 *
 * Tracks export batches, processing status, and timestamps
 * to enable incremental exports and consumer polling.
 */
import type { DbConnection } from '@crowd/database'

export interface JobMetrics {
  exportedRows?: number
  exportedBytes?: number
  transformedCount?: number
  skippedCount?: number
  transformSkippedCount?: number
  resolveSkippedCount?: number
  processingDurationMs?: number
}

export interface SnowflakeExportJob {
  id: number
  platform: string
  sourceName: string
  s3Path: string
  exportStartedAt: Date | null
  createdAt: Date
  updatedAt: Date
  processingStartedAt: Date | null
  completedAt: Date | null
  cleanedAt: Date | null
  error: string | null
  metrics: JobMetrics | null
}

export interface PlatformFilter {
  clause: string
  params: Record<string, unknown>
}

/**
 * Build a SQL platform filter for use in metadataStore queries.
 *
 * An empty array returns `AND FALSE` (matches nothing), preventing
 * accidental full-table scans when no platforms are configured.
 */
export function buildPlatformFilter(platforms: string[]): PlatformFilter {
  if (platforms.length === 0) {
    return { clause: 'AND FALSE', params: {} }
  }
  return {
    clause: 'AND platform = ANY($(platforms)::text[])',
    params: { platforms },
  }
}

export class MetadataStore {
  constructor(private readonly db: DbConnection) {}

  async insertExportJob(
    platform: string,
    sourceName: string,
    s3Path: string,
    totalRows: number,
    totalBytes: number,
    exportStartedAt: Date,
  ): Promise<void> {
    const metrics: JobMetrics = { exportedRows: totalRows, exportedBytes: totalBytes }
    await this.db.none(
      `INSERT INTO integration."snowflakeExportJobs" (platform, "sourceName", s3_path, "exportStartedAt", metrics)
       VALUES ($(platform), $(sourceName), $(s3Path), $(exportStartedAt), $(metrics)::jsonb)
       ON CONFLICT (s3_path) DO UPDATE SET
         "exportStartedAt" = EXCLUDED."exportStartedAt",
         "processingStartedAt" = NULL,
         "completedAt" = NULL,
         "cleanedAt" = NULL,
         error = NULL,
         metrics = EXCLUDED.metrics,
         "updatedAt" = NOW()`,
      { platform, sourceName, s3Path, exportStartedAt, metrics: JSON.stringify(metrics) },
    )
  }

  /**
   * Atomically claim the oldest pending job by setting processingStartedAt.
   * Uses FOR UPDATE SKIP LOCKED so concurrent consumers never pick the same row.
   */
  async claimOldestPendingJob(filter?: PlatformFilter): Promise<SnowflakeExportJob | null> {
    const platformFilter = filter?.clause ?? ''
    const params: Record<string, unknown> = filter?.params ?? {}
    const row = await this.db.oneOrNone<{
      id: number
      platform: string
      sourceName: string
      s3_path: string
      exportStartedAt: Date | null
      createdAt: Date
      updatedAt: Date
      processingStartedAt: Date | null
      completedAt: Date | null
      cleanedAt: Date | null
      error: string | null
      metrics: JobMetrics | null
    }>(
      `UPDATE integration."snowflakeExportJobs"
       SET "processingStartedAt" = NOW(), "updatedAt" = NOW()
       WHERE id = (
         SELECT id FROM integration."snowflakeExportJobs"
         WHERE "processingStartedAt" IS NULL
         ${platformFilter}
         ORDER BY "createdAt" ASC
         LIMIT 1
         FOR UPDATE SKIP LOCKED
       )
       RETURNING id, platform, "sourceName", s3_path, "exportStartedAt",
                 "createdAt", "updatedAt", "processingStartedAt", "completedAt", "cleanedAt", error, metrics`,
      params,
    )
    return row ? mapRowToJob(row) : null
  }

  async getCleanableJobS3Paths(
    intervalHours = 24,
    filter?: PlatformFilter,
    requireZeroSkipped = true,
  ): Promise<{ id: number; s3Path: string }[]> {
    const platformFilter = filter?.clause ?? ''
    const params: Record<string, unknown> = { intervalHours, ...filter?.params }
    const skippedFilter = requireZeroSkipped
      ? `AND metrics ? 'skippedCount' AND (metrics->>'skippedCount')::int = 0`
      : ''
    const rows = await this.db.manyOrNone<{ id: number; s3_path: string }>(
      `SELECT id, s3_path
       FROM integration."snowflakeExportJobs"
       WHERE "completedAt" IS NOT NULL
         AND "cleanedAt" IS NULL
         AND error IS NULL
         AND metrics IS NOT NULL
         ${skippedFilter}
         AND "completedAt" <= NOW() - make_interval(hours => $(intervalHours))
         ${platformFilter}
       ORDER BY "completedAt" ASC`,
      params,
    )
    return rows.map((r) => ({ id: r.id, s3Path: r.s3_path }))
  }

  async markCleaned(jobId: number): Promise<void> {
    await this.db.none(
      `UPDATE integration."snowflakeExportJobs"
       SET "cleanedAt" = NOW(), "updatedAt" = NOW()
       WHERE id = $(jobId)`,
      { jobId },
    )
  }

  /**
   * Release a previously claimed job by clearing processingStartedAt so it can
   * be re-claimed. Intended for dry-run or cancellation paths where the claim
   * should leave no trace; do not use for failures — use markFailed instead.
   */
  async releaseClaim(jobId: number): Promise<void> {
    await this.db.none(
      `UPDATE integration."snowflakeExportJobs"
       SET "processingStartedAt" = NULL, "updatedAt" = NOW()
       WHERE id = $(jobId)`,
      { jobId },
    )
  }

  async markCompleted(jobId: number, metrics?: Partial<JobMetrics>): Promise<void> {
    await this.db.none(
      `UPDATE integration."snowflakeExportJobs"
       SET "completedAt" = NOW(),
           metrics = COALESCE(metrics, '{}'::jsonb) || COALESCE($(metrics)::jsonb, '{}'::jsonb),
           "updatedAt" = NOW()
       WHERE id = $(jobId)`,
      { jobId, metrics: metrics ? JSON.stringify(metrics) : null },
    )
  }

  async markFailed(jobId: number, error: unknown, metrics?: Partial<JobMetrics>): Promise<void> {
    await this.db.none(
      `UPDATE integration."snowflakeExportJobs"
       SET error = $(error), "completedAt" = NOW(),
           metrics = COALESCE(metrics, '{}'::jsonb) || COALESCE($(metrics)::jsonb, '{}'::jsonb),
           "updatedAt" = NOW()
       WHERE id = $(jobId)`,
      { jobId, error: serializeJobError(error), metrics: metrics ? JSON.stringify(metrics) : null },
    )
  }

  async getLatestExportStartedAt(platform: string, sourceName: string): Promise<Date | null> {
    const row = await this.db.oneOrNone<{ max: Date | null }>(
      `SELECT MAX("exportStartedAt") AS max
       FROM integration."snowflakeExportJobs"
       WHERE platform = $(platform)
         AND "sourceName" = $(sourceName)
         AND "completedAt" IS NOT NULL
         AND error IS NULL`,
      { platform, sourceName },
    )
    return row?.max ?? null
  }
}

function serializeJobError(err: unknown): string {
  try {
    if (err instanceof Error) {
      const obj: Record<string, unknown> = {
        message: err.message,
        name: err.name,
        stack: err.stack,
      }
      // cause is non-enumerable on Error instances — capture it explicitly
      const cause = (err as { cause?: unknown }).cause
      if (cause !== undefined) {
        obj.cause = cause instanceof Error ? cause.message : cause
      }
      for (const key of Object.keys(err)) {
        if (!(key in obj)) obj[key] = (err as unknown as Record<string, unknown>)[key]
      }
      return JSON.stringify(obj)
    }
    // Non-Error throwables: preserve all fields if it's an object, otherwise wrap in {message}
    const payload = typeof err === 'object' && err !== null ? err : { message: String(err) }
    return JSON.stringify(payload)
  } catch {
    return JSON.stringify({ message: String(err) })
  }
}

function mapRowToJob(row: {
  id: number
  platform: string
  sourceName: string
  s3_path: string
  exportStartedAt: Date | null
  createdAt: Date
  updatedAt: Date
  processingStartedAt: Date | null
  completedAt: Date | null
  cleanedAt: Date | null
  error: string | null
  metrics: JobMetrics | null
}): SnowflakeExportJob {
  return {
    id: row.id,
    platform: row.platform,
    sourceName: row.sourceName,
    s3Path: row.s3_path,
    exportStartedAt: row.exportStartedAt,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
    processingStartedAt: row.processingStartedAt,
    completedAt: row.completedAt,
    cleanedAt: row.cleanedAt,
    error: row.error,
    metrics: row.metrics,
  }
}
