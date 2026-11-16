import type { QueryExecutor } from '@crowd/database'
import { DEFAULT_BATCH_SIZE } from '@crowd/snowflake'

export async function resolveSyncErrorsNotReportedByExport(
  qx: QueryExecutor,
  platform: string,
  exportStartedAt: Date,
): Promise<number> {
  return qx.result(
    `
    WITH export_files AS (
      SELECT "completedAt", error, (metrics->>'exportedRows')::int AS exported_rows
      FROM integration."snowflakeExportJobs"
      WHERE platform = $(platform)
        AND "exportStartedAt" = $(exportStartedAt)
    )
    UPDATE pcc_projects_sync_errors
    SET resolved = TRUE
    WHERE NOT resolved
      AND run_at < $(exportStartedAt)
      AND EXISTS (SELECT 1 FROM export_files WHERE exported_rows < $(batchSize))
      AND NOT EXISTS (
        SELECT 1 FROM export_files WHERE "completedAt" IS NULL OR error IS NOT NULL
      )
    `,
    { platform, exportStartedAt, batchSize: DEFAULT_BATCH_SIZE },
  )
}
