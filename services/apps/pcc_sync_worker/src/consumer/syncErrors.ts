import type { QueryExecutor } from '@crowd/database'
import { DEFAULT_BATCH_SIZE } from '@crowd/snowflake'

export async function resolveSyncErrorsNotReportedByExport(
  qx: QueryExecutor,
  platform: string,
  exportStartedAt: Date | string,
  errorTypesToKeep: string[] = [],
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
      AND error_type <> ALL($(errorTypesToKeep)::text[])
      AND EXISTS (SELECT 1 FROM export_files WHERE exported_rows < $(batchSize))
      AND NOT EXISTS (
        SELECT 1 FROM export_files WHERE "completedAt" IS NULL OR error IS NOT NULL
      )
    `,
    { platform, exportStartedAt, errorTypesToKeep, batchSize: DEFAULT_BATCH_SIZE },
  )
}

export async function resolveSyncErrorsOfProjects(
  qx: QueryExecutor,
  errorType: string,
  pccProjectIds: string[],
): Promise<number> {
  return qx.result(
    `
    UPDATE pcc_projects_sync_errors
    SET resolved = TRUE
    WHERE NOT resolved
      AND error_type = $(errorType)
      AND external_project_id = ANY($(pccProjectIds)::text[])
    `,
    { errorType, pccProjectIds },
  )
}

export async function isSingleFileExport(
  qx: QueryExecutor,
  platform: string,
  exportStartedAt: Date | string,
): Promise<boolean> {
  const { files, lastFiles }: { files: number; lastFiles: number } = await qx.selectOne(
    `
    SELECT
      COUNT(*)::int AS files,
      (COUNT(*) FILTER (WHERE (metrics->>'exportedRows')::int < $(batchSize)))::int AS "lastFiles"
    FROM integration."snowflakeExportJobs"
    WHERE platform = $(platform)
      AND "exportStartedAt" = $(exportStartedAt)
    `,
    { platform, exportStartedAt, batchSize: DEFAULT_BATCH_SIZE },
  )
  return files === 1 && lastFiles === 1
}
