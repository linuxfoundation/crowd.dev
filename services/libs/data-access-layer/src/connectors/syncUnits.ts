import type { QueryExecutor } from '../queryExecutor'

import type {
  IClaimedUnit,
  ISyncRunProgress,
  ISyncRunSuccess,
  ISyncUnit,
  SyncUnitUpsert,
} from './types'

const MIN_INITIAL_DELAY_SECONDS = 10
const MAX_INITIAL_DELAY_SECONDS = 900
const CLAIM_LEASE_MINUTES = 5
const ERROR_MESSAGE_MAX_LENGTH = 500

function truncateErrorMessage(message: string | null): string | null {
  if (!message) {
    return null
  }
  return message.slice(0, ERROR_MESSAGE_MAX_LENGTH)
}

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
     RETURNING su.id, su."integrationId", su.platform, su."syncName", su."channelId", su."channelName"`,
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
): Promise<void> {
  await qx.result(
    `UPDATE integration.sync_units
     SET "consecutiveFailures" = "consecutiveFailures" + 1,
         "lastErrorClass" = $(errorClass),
         "lastErrorMessage" = $(errorMessage),
         "lastRunComplete" = false,
         "lastRunAt" = now(),
         "nextRunAt" = $(nextRunAt),
         "lockedAt" = NULL,
         status = CASE WHEN $(deadLetterAfter)::int IS NOT NULL
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
    },
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
