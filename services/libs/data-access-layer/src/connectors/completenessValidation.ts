import type { QueryExecutor } from '../queryExecutor'
import type { CountWindow, IValidationUnit, ValidationTarget } from './types'

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
        ${target.integrationId ? `AND su."integrationId" = $(integrationId)` : ''}`
    case 'integration':
      return `su.status = 'active' AND su."integrationId" = $(integrationId)`
    case 'platform':
      return `su.status = 'active' AND su.platform = $(platform)`
    case 'enabled':
      return `su.status = 'active'`
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

  const source =
    target.kind === 'enabled'
      ? `integration.sync_unit_validations v JOIN integration.sync_units su ON su.id = v."unitId"`
      : `integration.sync_units su`
  const idColumn = target.kind === 'enabled' ? 'v."unitId"' : 'su.id'

  return qx.select(
    `SELECT su.id, su."integrationId", su.platform, su."channelId", su."channelName", su."syncName",
            ${COUNTER_KEYS_PROJECTION}
     FROM ${source}
     WHERE ${targetFilter(target)}
       AND ${INTEGRATION_NOT_DELETED}
       AND ($(afterId)::uuid IS NULL OR ${idColumn} > $(afterId)::uuid)
     ORDER BY ${idColumn}
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
  unitIds: string[],
  enabled: boolean,
  counterKeys: string[] | null,
): Promise<number> {
  if (unitIds.length === 0) {
    return 0
  }

  if (!enabled) {
    return qx.result(
      `DELETE FROM integration.sync_unit_validations WHERE "unitId" = ANY($(unitIds)::uuid[])`,
      { unitIds },
    )
  }

  return qx.tx(async (tx) => {
    await tx.result(
      `INSERT INTO integration.sync_unit_validations ("unitId")
       SELECT unnest($(unitIds)::uuid[])
       ON CONFLICT DO NOTHING`,
      { unitIds },
    )
    await tx.result(
      `DELETE FROM integration.sync_unit_validation_counters WHERE "unitId" = ANY($(unitIds)::uuid[])`,
      { unitIds },
    )
    if (counterKeys && counterKeys.length > 0) {
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
