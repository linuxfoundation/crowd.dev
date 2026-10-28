import type { QueryExecutor } from '../queryExecutor'
import type { CountWindow, IValidationUnit, ValidationTarget } from './types'

export interface IValidationResultUpsert {
  unitId: string
  counterKey: string
  activityTypes: string[]
  window: CountWindow
  sourceCount: number
  tinybirdCount: number
}

function targetFilter(target: ValidationTarget): string {
  switch (target.kind) {
    case 'units':
      return `status <> 'decommissioned' AND id = ANY($(unitIds)::uuid[])`
    case 'channel':
      return `status = 'active' AND lower("channelName") = lower($(channelName))`
    case 'integration':
      return `status = 'active' AND "integrationId" = $(integrationId)`
    case 'platform':
      return `status = 'active' AND platform = $(platform)`
    case 'enabled':
      return `status = 'active' AND "validationEnabled"`
  }
}

export async function listValidationUnitsPage(
  qx: QueryExecutor,
  target: ValidationTarget,
  page: { afterId?: string; limit: number },
): Promise<IValidationUnit[]> {
  if (target.kind === 'units' && target.unitIds.length === 0) {
    return []
  }

  return qx.select(
    `SELECT id, "integrationId", platform, "channelId", "channelName", "syncName",
            "validationCounterKeys"
     FROM integration.sync_units
     WHERE ${targetFilter(target)}
       AND EXISTS (
         SELECT 1
         FROM public.integrations i
         WHERE i.id = sync_units."integrationId" AND i."deletedAt" IS NULL
       )
       AND ($(afterId)::uuid IS NULL OR id > $(afterId)::uuid)
     ORDER BY id
     LIMIT $(limit)`,
    { ...target, afterId: page.afterId ?? null, limit: page.limit },
  )
}

function windowBounds(window: CountWindow): { windowStart: string; windowEnd: string } {
  return window.kind === 'all-time'
    ? { windowStart: '-infinity', windowEnd: 'infinity' }
    : { windowStart: window.from, windowEnd: window.to }
}

export async function upsertValidationResult(
  qx: QueryExecutor,
  result: IValidationResultUpsert,
): Promise<void> {
  const { window, ...fields } = result

  await qx.result(
    `INSERT INTO integration.sync_validation_results
       ("unitId", "counterKey", "activityTypes", "windowStart", "windowEnd",
        "sourceCount", "tinybirdCount", "checkedAt")
     VALUES
       ($(unitId), $(counterKey), $(activityTypes)::text[], $(windowStart)::timestamptz,
        $(windowEnd)::timestamptz, $(sourceCount), $(tinybirdCount), now())
     ON CONFLICT ("unitId", "counterKey", "windowStart", "windowEnd")
     DO UPDATE SET "activityTypes" = EXCLUDED."activityTypes",
                   "sourceCount" = EXCLUDED."sourceCount",
                   "tinybirdCount" = EXCLUDED."tinybirdCount",
                   "checkedAt" = now()`,
    { ...fields, ...windowBounds(window) },
  )
}

export async function setUnitsValidation(
  qx: QueryExecutor,
  unitIds: string[],
  enabled: boolean,
  counterKeys: string[] | null,
): Promise<number> {
  if (unitIds.length === 0) {
    return 0
  }

  return qx.result(
    `UPDATE integration.sync_units
     SET "validationEnabled" = $(enabled),
         "validationCounterKeys" = $(counterKeys)::text[]
     WHERE id = ANY($(unitIds)::uuid[])`,
    { unitIds, enabled, counterKeys },
  )
}
