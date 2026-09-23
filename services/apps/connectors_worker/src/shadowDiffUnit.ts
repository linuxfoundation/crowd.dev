import type { IShadowDiffUnit } from '@crowd/data-access-layer/src/connectors'
import { getShadowRecordsInWindow } from '@crowd/data-access-layer/src/connectors'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import {
  INangoRecord,
  NangoIntegration,
  NangoMetadataLastAction,
  getNangoCloudRecords,
  initNangoCloudClient,
} from '@crowd/nango'

import { getNangoModelForSync } from './nangoModelMapping'
import { fetchNangoRecordsInWindow } from './nangoWindowFetch'
import { IDiffableRecord, IShadowDiffMismatch, diffShadowAgainstNango } from './shadowDiff'

const MS_PER_DAY = 24 * 60 * 60 * 1000

export function previousDayWindow(now: Date = new Date()): { windowStart: Date; windowEnd: Date } {
  const windowEnd = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()))
  const windowStart = new Date(windowEnd.getTime() - MS_PER_DAY)
  return { windowStart, windowEnd }
}

export function resolveDiffWindow(targetDay?: string): {
  day: string
  windowStart: Date
  windowEnd: Date
} {
  if (targetDay) {
    const windowStart = new Date(`${targetDay}T00:00:00.000Z`)
    return { day: targetDay, windowStart, windowEnd: new Date(windowStart.getTime() + MS_PER_DAY) }
  }
  const { windowStart, windowEnd } = previousDayWindow(new Date(Date.now() - MS_PER_DAY))
  return { day: windowStart.toISOString().slice(0, 10), windowStart, windowEnd }
}

export function diffableRecordKey(record: { type: string; sourceId: string }): string {
  return `${record.type}::${record.sourceId}`
}

function toDiffableShadowRecord(record: {
  type: string
  sourceId: string
  data: Record<string, unknown>
}): IDiffableRecord {
  return { sourceId: record.sourceId, type: record.type, data: record.data }
}

function isDeletedNangoRecord(record: INangoRecord): boolean {
  return (
    record.metadata.lastAction === NangoMetadataLastAction.DELETED ||
    Boolean(record.metadata.deletedAt)
  )
}

function toDiffableNangoRecord(record: INangoRecord): IDiffableRecord | null {
  const activity = record.activity as Record<string, unknown> | null | undefined
  if (!activity || typeof activity.sourceId !== 'string' || typeof activity.type !== 'string') {
    return null
  }
  return { sourceId: activity.sourceId, type: activity.type, data: activity }
}

export interface IShadowDiffUnitResult {
  mismatches: IShadowDiffMismatch[]
  shadowKeys: { type: string; sourceId: string }[]
  nangoRecordsByKey: Map<string, INangoRecord>
}

export async function diffUnit(
  qx: QueryExecutor,
  unit: IShadowDiffUnit,
  connectionId: string,
  windowStart: Date,
  windowEnd: Date,
): Promise<IShadowDiffUnitResult> {
  const model = getNangoModelForSync(unit.syncName)
  if (!model) {
    return {
      mismatches: [
        {
          sourceId: unit.id,
          type: unit.syncName,
          kind: 'unsupported_sync',
          severity: 'high',
          syncName: unit.syncName,
        },
      ],
      shadowKeys: [],
      nangoRecordsByKey: new Map(),
    }
  }

  const shadowRecords = await getShadowRecordsInWindow(qx, unit.id, windowStart, windowEnd)

  await initNangoCloudClient()
  const nangoRecords = await fetchNangoRecordsInWindow<INangoRecord>(
    (cursor) =>
      getNangoCloudRecords(
        NangoIntegration.GITHUB,
        connectionId,
        model,
        cursor,
        undefined,
        windowStart.toISOString(),
      ),
    windowStart,
    windowEnd,
  )

  const diffableNangoRecords = nangoRecords
    .map(toDiffableNangoRecord)
    .filter((r): r is IDiffableRecord => r !== null)

  const deletedNangoKeys = new Set(
    nangoRecords
      .filter(isDeletedNangoRecord)
      .map(toDiffableNangoRecord)
      .filter((r): r is IDiffableRecord => r !== null)
      .map(diffableRecordKey),
  )

  const diffableShadowRecords = shadowRecords
    .map(toDiffableShadowRecord)
    .filter((r) => !deletedNangoKeys.has(diffableRecordKey(r)))

  const mismatches = diffShadowAgainstNango(
    diffableShadowRecords,
    diffableNangoRecords.filter((r) => !deletedNangoKeys.has(diffableRecordKey(r))),
  )

  const nangoRecordsByKey = new Map<string, INangoRecord>()
  for (const record of nangoRecords) {
    const diffable = toDiffableNangoRecord(record)
    if (diffable) {
      nangoRecordsByKey.set(diffableRecordKey(diffable), record)
    }
  }

  return {
    mismatches: mismatches.map((mismatch) => ({ ...mismatch, syncName: unit.syncName })),
    shadowKeys: shadowRecords.map((r) => ({ type: r.type, sourceId: r.sourceId })),
    nangoRecordsByKey,
  }
}

export function countMismatchesByKind(
  mismatches: IShadowDiffMismatch[],
): Record<IShadowDiffMismatch['kind'], number> {
  const counts: Record<IShadowDiffMismatch['kind'], number> = {
    missing_in_nango: 0,
    missing_in_shadow: 0,
    field_mismatch: 0,
    unsupported_sync: 0,
  }
  for (const mismatch of mismatches) {
    counts[mismatch.kind] += 1
  }
  return counts
}
