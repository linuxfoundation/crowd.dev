import type { QueryExecutor } from '../queryExecutor'
import { truncateErrorMessage } from '../utils'
import type {
  IClaimedUnit,
  ILifecycleChannel,
  ISyncRunProgress,
  ISyncRunSuccess,
  ISyncUnit,
  SyncUnitUpsert,
} from './types'

const MIN_INITIAL_DELAY_SECONDS = 10
const MAX_INITIAL_DELAY_SECONDS = 900
const CLAIM_LEASE_MINUTES = 5

export async function upsertSyncUnits(qx: QueryExecutor, units: SyncUnitUpsert[]): Promise<number> {
  if (units.length === 0) {
    return 0
  }

  const row = await qx.selectOne(
    `WITH up AS (
     INSERT INTO integration.sync_units AS su
       ("integrationId", platform, "channelId", "channelName", "syncName", watermark, "emitEnabled", "nextRunAt")
     SELECT u."integrationId", u.platform, u."channelId", u."channelName", u."syncName", u.watermark,
            COALESCE(u."emitEnabled", false),
            now() + COALESCE(
              u."initialDelaySeconds",
              $(minDelaySeconds) + random() * $(delaySpanSeconds)
            ) * interval '1 second'
     FROM unnest(
       $(integrationIds)::uuid[],
       $(platforms)::text[],
       $(channelIds)::text[],
       $(channelNames)::text[],
       $(syncNames)::text[],
       $(watermarks)::jsonb[],
       $(emitEnabled)::boolean[],
       $(initialDelaySeconds)::float8[]
     ) AS u("integrationId", platform, "channelId", "channelName", "syncName", watermark, "emitEnabled", "initialDelaySeconds")
     ON CONFLICT ("integrationId", "channelId", "syncName")
     DO UPDATE SET "channelName" = EXCLUDED."channelName",
                   watermark = COALESCE(EXCLUDED.watermark, su.watermark),
                   "updatedAt" = now()
     RETURNING (xmax = 0) AS inserted
     )
     SELECT count(*) FILTER (WHERE inserted)::int AS inserted FROM up`,
    {
      integrationIds: units.map((u) => u.integrationId),
      platforms: units.map((u) => u.platform),
      channelIds: units.map((u) => u.channelId),
      channelNames: units.map((u) => u.channelName),
      syncNames: units.map((u) => u.syncName),
      watermarks: units.map((u) => (u.watermark != null ? JSON.stringify(u.watermark) : null)),
      emitEnabled: units.map((u) => u.emitEnabled ?? null),
      initialDelaySeconds: units.map((u) => u.initialDelaySeconds ?? null),
      minDelaySeconds: MIN_INITIAL_DELAY_SECONDS,
      delaySpanSeconds: MAX_INITIAL_DELAY_SECONDS - MIN_INITIAL_DELAY_SECONDS,
    },
  )
  return row.inserted
}

export async function claimDueUnits(qx: QueryExecutor, limit: number): Promise<IClaimedUnit[]> {
  return qx.select(
    `UPDATE integration.sync_units su
     SET "lockedAt" = now(), "updatedAt" = now()
     FROM (
       SELECT su2.id, su2."lockedAt" IS NOT NULL AS "leaseLapsed"
       FROM integration.sync_units su2
       WHERE su2.status = 'active'
         AND su2."nextRunAt" <= now()
         AND (su2."lockedAt" IS NULL OR su2."lockedAt" < now() - $(leaseMinutes) * interval '1 minute')
         AND EXISTS (
           SELECT 1
           FROM public.integrations i
           WHERE i.id = su2."integrationId" AND i."deletedAt" IS NULL
         )
       ORDER BY su2."nextRunAt"
       LIMIT $(limit)
       FOR UPDATE SKIP LOCKED
     ) due
     WHERE su.id = due.id
     RETURNING su.id, su."integrationId", su.platform, su."syncName", su."channelId", su."channelName",
               due."leaseLapsed"`,
    { limit, leaseMinutes: CLAIM_LEASE_MINUTES },
  )
}

export async function rescheduleUnit(
  qx: QueryExecutor,
  id: string,
  nextRunAt: Date,
): Promise<void> {
  await qx.result(
    `UPDATE integration.sync_units
     SET "nextRunAt" = $(nextRunAt), "lockedAt" = NULL, "updatedAt" = now()
     WHERE id = $(id)`,
    { id, nextRunAt },
  )
}

// Keeps a still-running unit claimed so lockedAt stays set for the whole run. A NULL lockedAt
// means the run already recorded its end after this claim, so its own nextRunAt is kept.
export async function renewRunningUnitLease(qx: QueryExecutor, id: string): Promise<number> {
  return qx.result(
    `UPDATE integration.sync_units
     SET "lockedAt" = now(),
         "nextRunAt" = now() + $(leaseMinutes) * interval '1 minute',
         "updatedAt" = now()
     WHERE id = $(id) AND "lockedAt" IS NOT NULL`,
    { id, leaseMinutes: CLAIM_LEASE_MINUTES },
  )
}

// A previous run's end write can clear the lock between this run's claim and its start, so
// every run re-locks before reading its unit and stays locked until its own end write.
export async function reassertUnitClaim(qx: QueryExecutor, id: string): Promise<boolean> {
  const row = await qx.selectOneOrNone(
    `UPDATE integration.sync_units
     SET "lockedAt" = CASE WHEN status = 'active' THEN now() END, "updatedAt" = now()
     WHERE id = $(id)
     RETURNING status = 'active' AS claimed`,
    { id },
  )
  return row?.claimed === true
}

export async function recordRunSuccess(
  qx: QueryExecutor,
  id: string,
  data: ISyncRunSuccess,
  nextRunAt: Date,
): Promise<void> {
  await qx.result(
    `UPDATE integration.sync_units
     SET watermark = $(watermark)::jsonb,
         "emittedCount" = $(emittedCount),
         "nextRunAt" = $(nextRunAt),
         "lastRunAt" = now(),
         "lastSuccessAt" = now(),
         "consecutiveFailures" = 0,
         "lastErrorClass" = NULL,
         "lastErrorMessage" = NULL,
         "lastRunComplete" = $(complete),
         "lockedAt" = NULL,
         "updatedAt" = now()
     WHERE id = $(id)`,
    {
      id,
      watermark: JSON.stringify(data.watermark),
      emittedCount: data.emittedCount,
      complete: data.complete,
      nextRunAt,
    },
  )
}

export async function recordRunPartial(
  qx: QueryExecutor,
  id: string,
  progress: ISyncRunProgress,
  resumeAt: Date,
  errorClass: string,
  errorMessage: string | null,
): Promise<void> {
  await qx.result(
    `UPDATE integration.sync_units
     SET watermark = $(watermark)::jsonb,
         "emittedCount" = $(emittedCount),
         "nextRunAt" = $(resumeAt),
         "lastRunAt" = now(),
         "lastErrorClass" = $(errorClass),
         "lastErrorMessage" = $(errorMessage),
         "lastRunComplete" = false,
         "lockedAt" = NULL,
         "updatedAt" = now()
     WHERE id = $(id)`,
    {
      id,
      watermark: JSON.stringify(progress.watermark),
      emittedCount: progress.emittedCount,
      resumeAt,
      errorClass,
      errorMessage: truncateErrorMessage(errorMessage),
    },
  )
}

export async function parkUnit(
  qx: QueryExecutor,
  id: string,
  resumeAt: Date,
  errorClass: string,
  errorMessage: string | null,
): Promise<void> {
  await qx.result(
    `UPDATE integration.sync_units
     SET "nextRunAt" = $(resumeAt),
         "lastRunAt" = now(),
         "lastErrorClass" = $(errorClass),
         "lastErrorMessage" = $(errorMessage),
         "lastRunComplete" = false,
         "lockedAt" = NULL,
         "updatedAt" = now()
     WHERE id = $(id)`,
    { id, resumeAt, errorClass, errorMessage: truncateErrorMessage(errorMessage) },
  )
}

export async function recordRunFailure(
  qx: QueryExecutor,
  id: string,
  errorClass: string,
  errorMessage: string | null,
  deadLetterAfter: number | null,
  nextRunAt: Date,
  watermark: Record<string, unknown> | null = null,
): Promise<void> {
  await qx.result(
    `UPDATE integration.sync_units
     SET "consecutiveFailures" = "consecutiveFailures" + 1,
         watermark = COALESCE($(watermark)::jsonb, watermark),
         "lastErrorClass" = $(errorClass),
         "lastErrorMessage" = $(errorMessage),
         "lastRunComplete" = false,
         "lastRunAt" = now(),
         "nextRunAt" = $(nextRunAt),
         "lockedAt" = NULL,
         status = CASE WHEN status = 'active'
                        AND $(deadLetterAfter)::int IS NOT NULL
                        AND "consecutiveFailures" + 1 >= $(deadLetterAfter)
                       THEN 'dead_letter' ELSE status END,
         "updatedAt" = now()
     WHERE id = $(id)`,
    {
      id,
      errorClass,
      errorMessage: truncateErrorMessage(errorMessage),
      deadLetterAfter,
      nextRunAt,
      watermark: watermark != null ? JSON.stringify(watermark) : null,
    },
  )
}

export async function recordRunTimeout(
  qx: QueryExecutor,
  id: string,
  errorMessage: string,
  attemptStartedAt: Date,
): Promise<number> {
  return qx.result(
    `UPDATE integration.sync_units
     SET "consecutiveFailures" = "consecutiveFailures" + 1,
         "lastErrorClass" = 'worker.timeout',
         "lastErrorMessage" = $(errorMessage),
         "lastRunComplete" = false,
         "lastRunAt" = now(),
         "updatedAt" = now()
     WHERE id = $(id)
       AND ("lastRunAt" IS NULL OR "lastRunAt" < $(attemptStartedAt))`,
    { id, errorMessage: truncateErrorMessage(errorMessage), attemptStartedAt },
  )
}

export async function reviveDeadLetteredSyncUnits(
  qx: QueryExecutor,
  integrationId: string,
  syncName: string,
): Promise<number> {
  return qx.result(
    `UPDATE integration.sync_units
     SET status = 'active',
         "consecutiveFailures" = 0,
         "lastErrorClass" = NULL,
         "lastErrorMessage" = NULL,
         "nextRunAt" = now(),
         "lockedAt" = NULL,
         "updatedAt" = now()
     WHERE "integrationId" = $(integrationId)
       AND "syncName" = $(syncName)
       AND status = 'dead_letter'
       AND "lastErrorClass" = 'provider.auth'`,
    { integrationId, syncName },
  )
}

export async function getUnitById(qx: QueryExecutor, id: string): Promise<ISyncUnit | null> {
  return qx.selectOneOrNone(
    `SELECT *
     FROM integration.sync_units
     WHERE id = $(id)`,
    { id },
  )
}

export async function findSyncUnitByChannelId(
  qx: QueryExecutor,
  integrationId: string,
  platform: string,
  channelId: string,
  syncName: string,
): Promise<ISyncUnit | null> {
  return qx.selectOneOrNone(
    `SELECT *
     FROM integration.sync_units
     WHERE "integrationId" = $(integrationId)
       AND platform = $(platform)
       AND "channelId" = $(channelId)
       AND "syncName" = $(syncName)`,
    { integrationId, platform, channelId, syncName },
  )
}

// Keys already in the watermark are left alone and a running unit is skipped: the run holds the
// watermark it read at start and would commit over the patch.
export async function mergeSyncUnitWatermark(
  qx: QueryExecutor,
  id: string,
  patch: Record<string, unknown>,
): Promise<number> {
  return qx.result(
    `UPDATE integration.sync_units
     SET watermark = COALESCE(watermark, '{}'::jsonb) || $(patch)::jsonb,
         "updatedAt" = now()
     WHERE id = $(id)
       AND "lockedAt" IS NULL
       AND NOT (COALESCE(watermark, '{}'::jsonb) ?| $(keys)::text[])`,
    { id, patch: JSON.stringify(patch), keys: Object.keys(patch) },
  )
}

export async function reviveDecommissionedSyncUnit(qx: QueryExecutor, id: string): Promise<number> {
  return qx.result(
    `UPDATE integration.sync_units
     SET status = 'active', "nextRunAt" = now(), "updatedAt" = now()
     WHERE id = $(id) AND status = 'decommissioned'`,
    { id },
  )
}

export async function hasEmittingSyncUnit(
  qx: QueryExecutor,
  integrationId: string,
  channelName: string,
  syncName: string,
): Promise<boolean> {
  const row = await qx.selectOne(
    `SELECT EXISTS (
       SELECT 1
       FROM integration.sync_units
       WHERE "integrationId" = $(integrationId)
         AND lower("channelName") = lower($(channelName))
         AND "syncName" = $(syncName)
         AND "emitEnabled"
     ) AS exists`,
    { integrationId, channelName, syncName },
  )
  return row.exists === true
}

export async function listSyncUnitChannelNames(
  qx: QueryExecutor,
  integrationId: string,
  channelId: string,
): Promise<string[]> {
  const rows = await qx.select(
    `SELECT DISTINCT "channelName"
     FROM integration.sync_units
     WHERE "integrationId" = $(integrationId)
       AND "channelId" = $(channelId)`,
    { integrationId, channelId },
  )
  return rows.map((row: { channelName: string }) => row.channelName)
}

export async function lockSyncUnitChannel(qx: QueryExecutor, channelId: string): Promise<void> {
  await qx.selectOne(
    `SELECT pg_advisory_xact_lock(hashtextextended('syncUnitChannel:' || $(channelId), 0))`,
    { channelId },
  )
}

export async function alignSyncUnitChannelName(
  qx: QueryExecutor,
  integrationId: string,
  channelId: string,
  channelName: string,
): Promise<number> {
  return qx.result(
    `UPDATE integration.sync_units
     SET "channelName" = $(channelName)
     WHERE "integrationId" = $(integrationId)
       AND "channelId" = $(channelId)
       AND "channelName" <> $(channelName)
       AND "lockedAt" IS NULL`,
    { integrationId, channelId, channelName },
  )
}

export async function listSyncUnitChannelsForIntegration(
  qx: QueryExecutor,
  integrationId: string,
): Promise<ILifecycleChannel[]> {
  return qx.select(
    `SELECT min("channelName") AS "channelName",
            bool_or(status = 'decommissioned') AS "hasDecommissioned",
            bool_or("emitEnabled" AND status <> 'decommissioned') AS "hasEmitting"
     FROM integration.sync_units
     WHERE "integrationId" = $(integrationId)
     GROUP BY lower("channelName")
     ORDER BY 1`,
    { integrationId },
  )
}

export async function getEmittingChannelNamesForIntegrations(
  qx: QueryExecutor,
  integrationIds: string[],
): Promise<Record<string, string[]>> {
  if (integrationIds.length === 0) return {}

  const rows: { integrationId: string; channelName: string }[] = await qx.select(
    `SELECT "integrationId", min("channelName") AS "channelName"
     FROM integration.sync_units
     WHERE "integrationId" IN ($(integrationIds:csv))
       AND "emitEnabled"
       AND status <> 'decommissioned'
     GROUP BY "integrationId", lower("channelName")`,
    { integrationIds },
  )

  const result: Record<string, string[]> = {}
  for (const { integrationId, channelName } of rows) {
    const channelNames = result[integrationId] ?? []
    channelNames.push(channelName)
    result[integrationId] = channelNames
  }
  return result
}

export async function decommissionSyncUnitsForChannels(
  qx: QueryExecutor,
  integrationId: string,
  channelKeys: string[],
): Promise<number> {
  if (channelKeys.length === 0) {
    return 0
  }

  return qx.result(
    `UPDATE integration.sync_units
     SET status = 'decommissioned', "updatedAt" = now()
     WHERE "integrationId" = $(integrationId)
       AND status <> 'decommissioned'
       AND lower("channelName") = ANY($(channelKeys)::text[])`,
    { integrationId, channelKeys },
  )
}

// Credly's channelName is a renamable display title (see alignSyncUnitChannelName), unlike
// GitHub's stable repo-slug channelName, so a removed template is matched by channelId here.
export async function decommissionSyncUnitsForChannelIds(
  qx: QueryExecutor,
  integrationId: string,
  channelIds: string[],
): Promise<number> {
  if (channelIds.length === 0) {
    return 0
  }

  return qx.result(
    `UPDATE integration.sync_units
     SET status = 'decommissioned', "updatedAt" = now()
     WHERE "integrationId" = $(integrationId)
       AND status <> 'decommissioned'
       AND "channelId" = ANY($(channelIds)::text[])`,
    { integrationId, channelIds },
  )
}

export async function decommissionAllSyncUnits(
  qx: QueryExecutor,
  integrationId: string,
): Promise<number> {
  return qx.result(
    `UPDATE integration.sync_units
     SET status = 'decommissioned', "updatedAt" = now()
     WHERE "integrationId" = $(integrationId)
       AND status <> 'decommissioned'`,
    { integrationId },
  )
}

export async function countUndrainedDecommissionedSyncUnits(
  qx: QueryExecutor,
  integrationId: string,
  staleLockBefore: Date,
): Promise<number> {
  const row = await qx.selectOne(
    `SELECT count(*)::int AS count
     FROM integration.sync_units
     WHERE "integrationId" = $(integrationId)
       AND status = 'decommissioned'
       AND "lockedAt" IS NOT NULL
       AND "lockedAt" >= $(staleLockBefore)`,
    { integrationId, staleLockBefore },
  )
  return row.count
}

export async function deleteDrainedSyncUnits(
  qx: QueryExecutor,
  integrationId: string,
  staleLockBefore: Date,
): Promise<number> {
  return qx.result(
    `DELETE FROM integration.sync_units
     WHERE "integrationId" = $(integrationId)
       AND status = 'decommissioned'
       AND ("lockedAt" IS NULL OR "lockedAt" < $(staleLockBefore))`,
    { integrationId, staleLockBefore },
  )
}

export async function listDecommissionedSyncUnitChannels(
  qx: QueryExecutor,
  integrationId: string,
): Promise<string[]> {
  const rows = await qx.select(
    `SELECT DISTINCT lower("channelName") AS "channelKey"
     FROM integration.sync_units
     WHERE "integrationId" = $(integrationId)
       AND status = 'decommissioned'
     ORDER BY 1`,
    { integrationId },
  )
  return rows.map((row: { channelKey: string }) => row.channelKey)
}

export async function hasActiveSyncUnitsForChannelElsewhere(
  qx: QueryExecutor,
  integrationId: string,
  channelId: string,
  staleLockBefore: Date,
  platform: string,
): Promise<boolean> {
  const row = await qx.selectOne(
    `SELECT EXISTS (
       SELECT 1
       FROM integration.sync_units su
       JOIN public.integrations i ON i.id = su."integrationId"
       WHERE su."channelId" = $(channelId)
         AND su."integrationId" <> $(integrationId)
         AND i.platform = $(platform)
         AND (
           (su.status <> 'decommissioned' AND i."deletedAt" IS NULL)
           OR su."lockedAt" >= $(staleLockBefore)
         )
     ) AS exists`,
    { integrationId, channelId, platform, staleLockBefore },
  )
  return row.exists === true
}
