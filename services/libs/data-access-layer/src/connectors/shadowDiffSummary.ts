import type { QueryExecutor } from '../queryExecutor'

export interface ISyncDiffSummaryUpsert {
  unitId: string
  day: string
  integrationId: string
  channelName: string
  missingInNangoCount: number
  missingInShadowCount: number
  fieldMismatchCount: number
  unsupportedSyncCount: number
  highSeverityCount: number
}

export interface ISyncDiffDayCounts {
  day: string
  missingInShadowCount: number
  unsupportedSyncCount: number
}

export async function getSyncDiffCountsForDays(
  qx: QueryExecutor,
  unitId: string,
  days: string[],
): Promise<ISyncDiffDayCounts[]> {
  if (days.length === 0) {
    return []
  }

  return qx.select(
    `SELECT to_char(day, 'YYYY-MM-DD') AS day, "missingInShadowCount", "unsupportedSyncCount"
     FROM integration.sync_diff_summary
     WHERE "unitId" = $(unitId) AND day = ANY($(days)::date[])`,
    { unitId, days },
  )
}

export async function getUnitIdsWithSummary(
  qx: QueryExecutor,
  unitIds: string[],
  day: string,
): Promise<Set<string>> {
  if (unitIds.length === 0) {
    return new Set()
  }

  const rows = await qx.select(
    `SELECT "unitId" FROM integration.sync_diff_summary WHERE "unitId" = ANY($(unitIds)::uuid[]) AND day = $(day)`,
    { unitIds, day },
  )

  return new Set(rows.map((row) => row.unitId))
}

export async function upsertSyncDiffSummary(
  qx: QueryExecutor,
  summary: ISyncDiffSummaryUpsert,
): Promise<void> {
  const hasMismatch =
    summary.missingInNangoCount > 0 ||
    summary.missingInShadowCount > 0 ||
    summary.fieldMismatchCount > 0 ||
    summary.unsupportedSyncCount > 0

  await qx.result(
    `INSERT INTO integration.sync_diff_summary AS sds
       ("unitId", day, "integrationId", "channelName", "missingInNangoCount",
        "missingInShadowCount", "fieldMismatchCount", "unsupportedSyncCount",
        "highSeverityCount", "hasMismatch", "lastCheckedAt")
     VALUES
       ($(unitId), $(day), $(integrationId), $(channelName), $(missingInNangoCount),
        $(missingInShadowCount), $(fieldMismatchCount), $(unsupportedSyncCount),
        $(highSeverityCount), $(hasMismatch), now())
     ON CONFLICT ("unitId", day)
     DO UPDATE SET "integrationId" = EXCLUDED."integrationId",
                   "channelName" = EXCLUDED."channelName",
                   "missingInNangoCount" = EXCLUDED."missingInNangoCount",
                   "missingInShadowCount" = EXCLUDED."missingInShadowCount",
                   "fieldMismatchCount" = EXCLUDED."fieldMismatchCount",
                   "unsupportedSyncCount" = EXCLUDED."unsupportedSyncCount",
                   "highSeverityCount" = EXCLUDED."highSeverityCount",
                   "hasMismatch" = EXCLUDED."hasMismatch",
                   "lastCheckedAt" = now()`,
    { ...summary, hasMismatch },
  )
}
