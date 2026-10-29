import type { QueryExecutor } from '../queryExecutor'
import type { CountWindow, IValidationChannel, IValidationUnit, ValidationTarget } from './types'

export interface IValidationResultUpsert {
  unitId: string
  counterKey: string
  window: CountWindow
  sourceCount: number
  tinybirdCount: number
}

const INTEGRATION_NOT_DELETED = `EXISTS (
  SELECT 1
  FROM public.integrations i
  WHERE i.id = su."integrationId" AND i."deletedAt" IS NULL
)`

const NOT_DISABLED = `NOT EXISTS (
  SELECT 1
  FROM integration.sync_unit_validations v
  WHERE v."unitId" = su.id AND NOT v.enabled
)`

const COUNTER_KEYS_PROJECTION = `(
  SELECT array_agg(c."counterKey" ORDER BY c."counterKey")
  FROM integration.sync_unit_validation_counters c
  WHERE c."unitId" = su.id
) AS "validationCounterKeys"`

function targetFilter(target: ValidationTarget): string {
  switch (target.kind) {
    case 'units':
      return `su.status <> 'decommissioned' AND su.id = ANY($(unitIds)::uuid[])`
    case 'channel':
      return `su.status = 'active' AND lower(su."channelName") = lower($(channelName))
        ${target.integrationId !== undefined ? `AND su."integrationId" = $(integrationId)` : ''}`
    case 'integration':
      return `su.status = 'active' AND su."integrationId" = $(integrationId)`
    case 'platform':
      return `su.status = 'active' AND su.platform = $(platform)`
  }
}

export async function listValidationUnitsPage(
  qx: QueryExecutor,
  target: ValidationTarget,
  page: { afterId?: string; limit: number; includeDisabled?: boolean },
): Promise<IValidationUnit[]> {
  if (target.kind === 'units' && target.unitIds.length === 0) {
    return []
  }

  const disabledFilter =
    target.kind === 'units' || page.includeDisabled ? '' : `AND ${NOT_DISABLED}`

  return qx.select(
    `SELECT su.id, su."integrationId", su.platform, su."channelId", su."channelName", su."syncName",
            ${COUNTER_KEYS_PROJECTION},
            COALESCE((SELECT v.enabled FROM integration.sync_unit_validations v WHERE v."unitId" = su.id), true) AS "validationEnabled"
     FROM integration.sync_units su
     WHERE ${targetFilter(target)}
       ${disabledFilter}
       AND ${INTEGRATION_NOT_DELETED}
       AND ($(afterId)::uuid IS NULL OR su.id > $(afterId)::uuid)
     ORDER BY su.id
     LIMIT $(limit)`,
    { ...target, afterId: page.afterId ?? null, limit: page.limit },
  )
}

export async function listValidationChannelsPage(
  qx: QueryExecutor,
  page: { after?: Pick<IValidationChannel, 'integrationId' | 'channelId'>; limit: number },
): Promise<IValidationChannel[]> {
  return qx.select(
    `SELECT su."integrationId", su."channelId", min(su."channelName") AS "channelName"
     FROM integration.sync_units su
     WHERE su.status = 'active'
       AND ${NOT_DISABLED}
       AND ${INTEGRATION_NOT_DELETED}
       AND ($(afterIntegrationId)::uuid IS NULL
            OR (su."integrationId", su."channelId") > ($(afterIntegrationId)::uuid, $(afterChannelId)::text))
     GROUP BY su."integrationId", su."channelId"
     ORDER BY su."integrationId", su."channelId"
     LIMIT $(limit)`,
    {
      afterIntegrationId: page.after?.integrationId ?? null,
      afterChannelId: page.after?.channelId ?? null,
      limit: page.limit,
    },
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
       ("unitId", "counterKey", "windowStart", "windowEnd",
        "sourceCount", "tinybirdCount", "checkedAt")
     VALUES
       ($(unitId), $(counterKey), $(windowStart)::timestamptz,
        $(windowEnd)::timestamptz, $(sourceCount), $(tinybirdCount), now())
     ON CONFLICT ("unitId", "counterKey", "windowStart", "windowEnd")
     DO UPDATE SET "sourceCount" = EXCLUDED."sourceCount",
                   "tinybirdCount" = EXCLUDED."tinybirdCount",
                   "checkedAt" = now()`,
    { ...fields, ...windowBounds(window) },
  )
}

export async function setUnitsValidation(
  qx: QueryExecutor,
  requestedUnitIds: string[],
  enabled: boolean,
  requestedCounterKeys: string[] | null,
): Promise<number> {
  if (requestedUnitIds.length === 0) {
    return 0
  }

  const unitIds = [...new Set(requestedUnitIds)].sort()
  const counterKeys = requestedCounterKeys ? [...new Set(requestedCounterKeys)] : null

  return qx.tx(async (tx) => {
    await tx.result(
      `INSERT INTO integration.sync_unit_validations ("unitId", enabled)
       SELECT unnest($(unitIds)::uuid[]), $(enabled)
       ON CONFLICT ("unitId") DO UPDATE SET enabled = EXCLUDED.enabled`,
      { unitIds, enabled },
    )
    await tx.result(
      `DELETE FROM integration.sync_unit_validation_counters WHERE "unitId" = ANY($(unitIds)::uuid[])`,
      { unitIds },
    )
    if (enabled && counterKeys && counterKeys.length > 0) {
      await tx.result(
        `INSERT INTO integration.sync_unit_validation_counters ("unitId", "counterKey")
         SELECT u, k FROM unnest($(unitIds)::uuid[]) u CROSS JOIN unnest($(counterKeys)::text[]) k`,
        { unitIds, counterKeys },
      )
    }
    const row = await tx.selectOne(
      `SELECT count(*)::int AS count
       FROM integration.sync_unit_validations
       WHERE "unitId" = ANY($(unitIds)::uuid[])`,
      { unitIds },
    )
    return row.count
  })
}
