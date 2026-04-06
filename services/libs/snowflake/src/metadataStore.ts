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
  async claimOldestPendingJob(
    platform?: string,
    platforms?: string[],
  ): Promise<SnowflakeExportJob | null> {
    let platformFilter = ''
    let params: Record<string, unknown> = {}
    if (platform) {
      platformFilter = 'AND platform = $(platform)'
      params = { platform }
    } else if (platforms && platforms.length > 0) {
      platformFilter = 'AND platform = ANY($(platforms)::text[])'
      params = { platforms }
    }
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
    platform?: string,
    requireZeroSkipped = true,
    platforms?: string[],
  ): Promise<{ id: number; s3Path: string }[]> {
    let platformFilter = ''
    const params: { intervalHours: number; platform?: string; platforms?: string[] } = {
      intervalHours,
    }
    if (platform) {
      platformFilter = 'AND platform = $(platform)'
      params.platform = platform
    } else if (platforms && platforms.length > 0) {
      platformFilter = 'AND platform = ANY($(platforms)::text[])'
      params.platforms = platforms
    }
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

  async markFailed(jobId: number, error: string, metrics?: Partial<JobMetrics>): Promise<void> {
    await this.db.none(
      `UPDATE integration."snowflakeExportJobs"
       SET error = $(error), "completedAt" = NOW(),
           metrics = COALESCE(metrics, '{}'::jsonb) || COALESCE($(metrics)::jsonb, '{}'::jsonb),
           "updatedAt" = NOW()
       WHERE id = $(jobId)`,
      { jobId, error, metrics: metrics ? JSON.stringify(metrics) : null },
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
