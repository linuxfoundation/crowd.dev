import type { QueryExecutor } from '../queryExecutor'

import type { IShadowRecord } from './types'

export async function recordShadowRecords(
  qx: QueryExecutor,
  unitId: string,
  records: IShadowRecord[],
): Promise<void> {
  if (records.length === 0) {
    return
  }

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
      types: records.map((r) => r.type),
      sourceIds: records.map((r) => r.sourceId),
      occurredAts: records.map((r) => r.occurredAt),
      data: records.map((r) => JSON.stringify(r.data)),
    },
  )
}
