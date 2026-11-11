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
