import type { QueryExecutor } from '../queryExecutor'

import type { IClaimedUnit, ISyncRunSuccess, ISyncUnit, SyncUnitUpsert } from './types'

const MIN_INITIAL_DELAY_SECONDS = 10
const MAX_INITIAL_DELAY_SECONDS = 900
const CLAIM_LEASE_MINUTES = 5

export async function upsertSyncUnits(qx: QueryExecutor, units: SyncUnitUpsert[]): Promise<number> {
  if (units.length === 0) {
    return 0
  }

  return qx.result(
    `INSERT INTO integration.sync_units
       ("integrationId", platform, "channelId", "channelName", "syncName", "nextRunAt")
     SELECT u.*, now() + ($(minDelaySeconds) + random() * $(delaySpanSeconds)) * interval '1 second'
     FROM unnest(
       $(integrationIds)::uuid[],
       $(platforms)::text[],
       $(channelIds)::text[],
       $(channelNames)::text[],
       $(syncNames)::text[]
     ) u
     ON CONFLICT ("integrationId", "channelId", "syncName")
     DO UPDATE SET "channelName" = EXCLUDED."channelName", "updatedAt" = now()`,
    {
      integrationIds: units.map((u) => u.integrationId),
      platforms: units.map((u) => u.platform),
      channelIds: units.map((u) => u.channelId),
      channelNames: units.map((u) => u.channelName),
      syncNames: units.map((u) => u.syncName),
      minDelaySeconds: MIN_INITIAL_DELAY_SECONDS,
      delaySpanSeconds: MAX_INITIAL_DELAY_SECONDS - MIN_INITIAL_DELAY_SECONDS,
    },
  )
}

export async function claimDueUnits(qx: QueryExecutor, limit: number): Promise<IClaimedUnit[]> {
  return qx.select(
    `UPDATE integration.sync_units su
     SET "lockedAt" = now(), "updatedAt" = now()
     WHERE su.id IN (
       SELECT su2.id
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
     )
     RETURNING su.id, su."integrationId", su.platform, su."syncName"`,
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

export async function recordRunSuccess(
  qx: QueryExecutor,
  id: string,
  data: ISyncRunSuccess,
): Promise<void> {
  await qx.result(
    `UPDATE integration.sync_units
     SET watermark = $(watermark)::jsonb,
         "emittedCount" = $(emittedCount),
         "lastRunAt" = now(),
         "lastSuccessAt" = now(),
         "consecutiveFailures" = 0,
         "updatedAt" = now()
     WHERE id = $(id)`,
    { id, watermark: JSON.stringify(data.watermark), emittedCount: data.emittedCount },
  )
}

export async function recordRunFailure(
  qx: QueryExecutor,
  id: string,
  errorClass: string,
  deadLetterAfter: number,
): Promise<void> {
  await qx.result(
    `UPDATE integration.sync_units
     SET "consecutiveFailures" = "consecutiveFailures" + 1,
         "lastErrorClass" = $(errorClass),
         "lastRunAt" = now(),
         status = CASE WHEN "consecutiveFailures" + 1 >= $(deadLetterAfter)
                       THEN 'dead_letter' ELSE status END,
         "updatedAt" = now()
     WHERE id = $(id)`,
    { id, errorClass, deadLetterAfter },
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
