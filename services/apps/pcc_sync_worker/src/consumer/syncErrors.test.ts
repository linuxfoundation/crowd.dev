import { test as base, describe, expect } from 'vitest'

import type { QueryExecutor } from '@crowd/database'
import { DEFAULT_BATCH_SIZE } from '@crowd/snowflake'
import { withQx } from '@crowd/test-kit/db'

import { resolveSyncErrorsNotReportedByExport } from './syncErrors'

const test = withQx(base)

const PLATFORM = 'pcc'
const EXPORT_STARTED_AT = new Date('2026-11-16T01:00:00.000Z')
const BEFORE_EXPORT = new Date('2026-11-15T01:05:00.000Z')
const DURING_EXPORT = new Date('2026-11-16T01:05:00.000Z')

async function insertSyncError(
  qx: QueryExecutor,
  externalProjectId: string | null,
  runAt: Date,
): Promise<void> {
  await qx.result(
    `
    INSERT INTO pcc_projects_sync_errors (external_project_id, error_type, details, run_at)
    VALUES ($(externalProjectId), 'HIERARCHY_MISMATCH', '{"reason": "moved"}'::jsonb, $(runAt))
    `,
    { externalProjectId, runAt },
  )
}

async function insertExportJob(
  qx: QueryExecutor,
  file: string,
  state: 'completed' | 'pending' | 'failed',
  exportStartedAt: Date = EXPORT_STARTED_AT,
  exportedRows = DEFAULT_BATCH_SIZE - 1,
): Promise<void> {
  await qx.result(
    `
    INSERT INTO integration."snowflakeExportJobs"
      (platform, "sourceName", s3_path, "exportStartedAt", "completedAt", error, metrics)
    VALUES
      ($(platform), 'project-hierarchy', $(s3Path), $(exportStartedAt), $(completedAt), $(error),
       $(metrics)::jsonb)
    `,
    {
      platform: PLATFORM,
      s3Path: `s3://pcc/${exportStartedAt.toISOString()}/${file}`,
      exportStartedAt,
      completedAt: state === 'pending' ? null : DURING_EXPORT,
      error: state === 'failed' ? '{"message": "boom"}' : null,
      metrics: JSON.stringify({ exportedRows }),
    },
  )
}

async function unresolvedProjectIds(qx: QueryExecutor): Promise<string[]> {
  const rows = await qx.select(
    `SELECT external_project_id FROM pcc_projects_sync_errors WHERE NOT resolved ORDER BY 1`,
  )
  return rows.map((row: { external_project_id: string }) => row.external_project_id)
}

describe('resolveSyncErrorsNotReportedByExport', () => {
  test('resolves the errors the export did not report again', async ({ qx }) => {
    await insertExportJob(qx, 'part-0.parquet', 'completed')
    await insertSyncError(qx, 'pcc-fixed', BEFORE_EXPORT)
    await insertSyncError(qx, 'pcc-still-broken', DURING_EXPORT)

    const resolved = await resolveSyncErrorsNotReportedByExport(qx, PLATFORM, EXPORT_STARTED_AT)

    expect(resolved).toBe(1)
    expect(await unresolvedProjectIds(qx)).toEqual(['pcc-still-broken'])
  })

  test('resolves errors that have no project id', async ({ qx }) => {
    await insertExportJob(qx, 'part-0.parquet', 'completed')
    await insertSyncError(qx, null, BEFORE_EXPORT)

    const resolved = await resolveSyncErrorsNotReportedByExport(qx, PLATFORM, EXPORT_STARTED_AT)

    expect(resolved).toBe(1)
  })

  test('resolves nothing while another file of the same export is pending', async ({ qx }) => {
    await insertExportJob(qx, 'part-0.parquet', 'completed')
    await insertExportJob(qx, 'part-1.parquet', 'pending')
    await insertSyncError(qx, 'pcc-in-the-other-file', BEFORE_EXPORT)

    const resolved = await resolveSyncErrorsNotReportedByExport(qx, PLATFORM, EXPORT_STARTED_AT)

    expect(resolved).toBe(0)
    expect(await unresolvedProjectIds(qx)).toEqual(['pcc-in-the-other-file'])
  })

  test('resolves nothing until the last file of the export has been written', async ({ qx }) => {
    await insertExportJob(qx, 'part-0.parquet', 'completed', EXPORT_STARTED_AT, DEFAULT_BATCH_SIZE)
    await insertSyncError(qx, 'pcc-in-a-later-file', BEFORE_EXPORT)

    const resolved = await resolveSyncErrorsNotReportedByExport(qx, PLATFORM, EXPORT_STARTED_AT)

    expect(resolved).toBe(0)
  })

  test('resolves nothing when a file of the same export failed', async ({ qx }) => {
    await insertExportJob(qx, 'part-0.parquet', 'completed')
    await insertExportJob(qx, 'part-1.parquet', 'failed')
    await insertSyncError(qx, 'pcc-in-the-failed-file', BEFORE_EXPORT)

    const resolved = await resolveSyncErrorsNotReportedByExport(qx, PLATFORM, EXPORT_STARTED_AT)

    expect(resolved).toBe(0)
  })

  test('ignores pending files that belong to another export', async ({ qx }) => {
    await insertExportJob(qx, 'part-0.parquet', 'completed')
    await insertExportJob(qx, 'part-0.parquet', 'pending', new Date('2026-11-17T01:00:00.000Z'))
    await insertSyncError(qx, 'pcc-fixed', BEFORE_EXPORT)

    const resolved = await resolveSyncErrorsNotReportedByExport(qx, PLATFORM, EXPORT_STARTED_AT)

    expect(resolved).toBe(1)
  })

  test('lets a resolved error be recorded again as unresolved', async ({ qx }) => {
    await insertExportJob(qx, 'part-0.parquet', 'completed')
    await insertSyncError(qx, 'pcc-comes-back', BEFORE_EXPORT)
    await resolveSyncErrorsNotReportedByExport(qx, PLATFORM, EXPORT_STARTED_AT)

    await insertSyncError(qx, 'pcc-comes-back', DURING_EXPORT)

    expect(await unresolvedProjectIds(qx)).toEqual(['pcc-comes-back'])
  })
})
