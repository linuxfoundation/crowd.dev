import type { QueryExecutor } from '../queryExecutor'
import type { IShadowRecord } from './types'

const stripNullBytes = (_key: string, value: unknown): unknown =>
  typeof value === 'string' ? value.replace(/\0/g, '') : value

export async function recordShadowRecords(
  qx: QueryExecutor,
  unitId: string,
  records: IShadowRecord[],
): Promise<void> {
  if (records.length === 0) {
    return
  }

  const byKey = new Map(records.map((r) => [JSON.stringify([r.type, r.sourceId]), r]))
  const deduped = [...byKey.values()]

  await qx.result(
    `INSERT INTO integration.sync_shadow_records
       ("unitId", type, "sourceId", "occurredAt", data)
     SELECT $(unitId)::uuid, u.*
     FROM unnest(
       $(types)::text[],
       $(sourceIds)::text[],
       $(occurredAts)::timestamptz[],
       $(data)::jsonb[]
     ) u
     ON CONFLICT ("unitId", type, "sourceId")
     DO UPDATE SET data = EXCLUDED.data, "occurredAt" = EXCLUDED."occurredAt"`,
    {
      unitId,
      types: deduped.map((r) => r.type),
      sourceIds: deduped.map((r) => r.sourceId),
      occurredAts: deduped.map((r) => r.occurredAt),
      data: deduped.map((r) => JSON.stringify(r.data, stripNullBytes)),
    },
  )
}

export async function getShadowRecordsInWindow(
  qx: QueryExecutor,
  unitId: string,
  windowStart: Date,
  windowEnd: Date,
): Promise<IShadowRecord[]> {
  return qx.select(
    `SELECT type, "sourceId", "occurredAt", data
     FROM integration.sync_shadow_records
     WHERE "unitId" = $(unitId)
       AND "occurredAt" >= $(windowStart)
       AND "occurredAt" < $(windowEnd)`,
    { unitId, windowStart, windowEnd },
  )
}

export async function pruneMatchingShadowRecords(
  qx: QueryExecutor,
  unitId: string,
  windowStart: Date,
  windowEnd: Date,
  keysToDelete: { type: string; sourceId: string }[],
): Promise<number> {
  if (keysToDelete.length === 0) {
    return 0
  }

  return qx.result(
    `DELETE FROM integration.sync_shadow_records sr
     WHERE sr."unitId" = $(unitId)
       AND sr."occurredAt" >= $(windowStart)
       AND sr."occurredAt" < $(windowEnd)
       AND EXISTS (
         SELECT 1
         FROM unnest($(deleteTypes)::text[], $(deleteSourceIds)::text[]) AS k(type, "sourceId")
         WHERE k.type = sr.type AND k."sourceId" = sr."sourceId"
       )`,
    {
      unitId,
      windowStart,
      windowEnd,
      deleteTypes: keysToDelete.map((k) => k.type),
      deleteSourceIds: keysToDelete.map((k) => k.sourceId),
    },
  )
}
