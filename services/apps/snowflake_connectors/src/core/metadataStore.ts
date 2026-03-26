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
       VALUES ($1, $2, $3, $4, $5::jsonb)
       ON CONFLICT (s3_path) DO UPDATE SET
         "exportStartedAt" = EXCLUDED."exportStartedAt",
         "processingStartedAt" = NULL,
         "completedAt" = NULL,
         "cleanedAt" = NULL,
         error = NULL,
         metrics = EXCLUDED.metrics,
         "updatedAt" = NOW()`,
      [platform, sourceName, s3Path, exportStartedAt, JSON.stringify(metrics)],
    )
  }

  /**
   * Atomically claim the oldest pending job by setting processingStartedAt.
   * Uses FOR UPDATE SKIP LOCKED so concurrent consumers never pick the same row.
   */
  async claimOldestPendingJob(): Promise<SnowflakeExportJob | null> {
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
         ORDER BY "createdAt" ASC
         LIMIT 1
         FOR UPDATE SKIP LOCKED
       )
       RETURNING id, platform, "sourceName", s3_path, "exportStartedAt",
                 "createdAt", "updatedAt", "processingStartedAt", "completedAt", "cleanedAt", error, metrics`,
    )
    return row ? mapRowToJob(row) : null
  }

  async getCleanableJobS3Paths(intervalHours = 24): Promise<{ id: number; s3Path: string }[]> {
    const rows = await this.db.manyOrNone<{ id: number; s3_path: string }>(
      `SELECT id, s3_path
       FROM integration."snowflakeExportJobs"
       WHERE "completedAt" IS NOT NULL
         AND "cleanedAt" IS NULL
         AND error IS NULL
         AND metrics IS NOT NULL
         AND metrics ? 'skippedCount'
         AND (metrics->>'skippedCount')::int = 0
         AND "completedAt" <= NOW() - make_interval(hours => $1)
       ORDER BY "completedAt" ASC`,
      [intervalHours],
    )
    return rows.map((r) => ({ id: r.id, s3Path: r.s3_path }))
  }

  async markCleaned(jobId: number): Promise<void> {
    await this.db.none(
      `UPDATE integration."snowflakeExportJobs"
       SET "cleanedAt" = NOW(), "updatedAt" = NOW()
       WHERE id = $1`,
      [jobId],
    )
  }

  async markCompleted(jobId: number, metrics?: Partial<JobMetrics>): Promise<void> {
    await this.db.none(
      `UPDATE integration."snowflakeExportJobs"
       SET "completedAt" = NOW(),
           metrics = COALESCE(metrics, '{}'::jsonb) || COALESCE($2::jsonb, '{}'::jsonb),
           "updatedAt" = NOW()
       WHERE id = $1`,
      [jobId, metrics ? JSON.stringify(metrics) : null],
    )
  }

  async markFailed(jobId: number, error: string, metrics?: Partial<JobMetrics>): Promise<void> {
    await this.db.none(
      `UPDATE integration."snowflakeExportJobs"
       SET error = $2, "completedAt" = NOW(),
           metrics = COALESCE(metrics, '{}'::jsonb) || COALESCE($3::jsonb, '{}'::jsonb),
           "updatedAt" = NOW()
       WHERE id = $1`,
      [jobId, error, metrics ? JSON.stringify(metrics) : null],
    )
  }

  async getLatestExportStartedAt(platform: string, sourceName: string): Promise<Date | null> {
    const row = await this.db.oneOrNone<{ max: Date | null }>(
      `SELECT MAX("exportStartedAt") AS max
       FROM integration."snowflakeExportJobs"
       WHERE platform = $1
         AND "sourceName" = $2
         AND "completedAt" IS NOT NULL
         AND error IS NULL`,
      [platform, sourceName],
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
