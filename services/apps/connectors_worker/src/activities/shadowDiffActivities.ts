import { parseRepoChannel } from '@crowd/connectors/src/connectors/github/paging'
import {
  IShadowDiffUnit,
  getShadowRecordsInWindow,
  getUnitIdsWithSummary,
  listShadowDiffUnits,
  pruneMatchingShadowRecords,
  upsertSyncDiffSummary,
} from '@crowd/data-access-layer/src/connectors'
import { getNangoMappingForRepo } from '@crowd/data-access-layer/src/integrations'
import { dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'
import {
  INangoRecord,
  NangoIntegration,
  NangoMetadataLastAction,
  getNangoCloudRecords,
  initNangoCloudClient,
} from '@crowd/nango'

import { svc } from '../main'
import { getNangoModelForSync } from '../nangoModelMapping'
import { fetchNangoRecordsInWindow } from '../nangoWindowFetch'
import {
  IDiffableRecord,
  IShadowDiffMismatch,
  ShadowDiffMismatchKind,
  diffShadowAgainstNango,
} from '../shadowDiff'

const MS_PER_DAY = 24 * 60 * 60 * 1000

export interface IShadowDiffChannel {
  channelName: string
  integrationId: string
  units: IShadowDiffUnit[]
}

export type ShadowDiffChannelStatus = 'ok' | 'mapping_missing' | 'error'

export interface IShadowDiffChannelResult {
  channelName: string
  integrationId: string
  status: ShadowDiffChannelStatus
  errorMessage?: string
}

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

export async function listShadowDiffChannels(): Promise<IShadowDiffChannel[]> {
  const units = await listShadowDiffUnits(dbStoreQx(svc.postgres.writer))

  const byChannel = new Map<string, IShadowDiffChannel>()
  for (const unit of units) {
    const key = `${unit.integrationId}::${unit.channelName}`
    const existing = byChannel.get(key)
    if (existing) {
      existing.units.push(unit)
    } else {
      byChannel.set(key, {
        channelName: unit.channelName,
        integrationId: unit.integrationId,
        units: [unit],
      })
    }
  }

  return [...byChannel.values()]
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

function diffableRecordKey(record: { type: string; sourceId: string }): string {
  return `${record.type}::${record.sourceId}`
}

interface IShadowDiffUnitResult {
  mismatches: IShadowDiffMismatch[]
  shadowKeys: { type: string; sourceId: string }[]
}

async function diffUnit(
  qx: ReturnType<typeof dbStoreQx>,
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

  return {
    mismatches: mismatches.map((mismatch) => ({ ...mismatch, syncName: unit.syncName })),
    shadowKeys: shadowRecords.map((r) => ({ type: r.type, sourceId: r.sourceId })),
  }
}

function countMismatchesByKind(
  mismatches: IShadowDiffMismatch[],
): Record<ShadowDiffMismatchKind, number> {
  const counts: Record<ShadowDiffMismatchKind, number> = {
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

async function persistUnitDiffResult(
  qx: ReturnType<typeof dbStoreQx>,
  channel: IShadowDiffChannel,
  unit: IShadowDiffUnit,
  day: string,
  windowStart: Date,
  windowEnd: Date,
  { mismatches, shadowKeys }: IShadowDiffUnitResult,
): Promise<void> {
  const counts = countMismatchesByKind(mismatches)

  await upsertSyncDiffSummary(qx, {
    unitId: unit.id,
    day,
    integrationId: channel.integrationId,
    channelName: channel.channelName,
    missingInNangoCount: counts.missing_in_nango,
    missingInShadowCount: counts.missing_in_shadow,
    fieldMismatchCount: counts.field_mismatch,
    unsupportedSyncCount: counts.unsupported_sync,
    highSeverityCount: mismatches.filter((m) => m.severity === 'high').length,
  })

  if (counts.unsupported_sync > 0) {
    return
  }

  const keysWithUnresolvedMismatch = new Set(
    mismatches
      .filter((m) => m.kind === 'field_mismatch' || m.kind === 'missing_in_nango')
      .map((m) => diffableRecordKey(m)),
  )

  const keysToDelete = shadowKeys.filter(
    (key) => !keysWithUnresolvedMismatch.has(diffableRecordKey(key)),
  )

  await pruneMatchingShadowRecords(qx, unit.id, windowStart, windowEnd, keysToDelete)
}

export async function runShadowDiffForChannel(
  channel: IShadowDiffChannel,
  targetDay?: string,
): Promise<IShadowDiffChannelResult> {
  const qx = dbStoreQx(svc.postgres.writer)

  const { owner, repo } = parseRepoChannel(channel.channelName)
  const mapping = await getNangoMappingForRepo(qx, channel.integrationId, owner, repo)

  if (!mapping) {
    return {
      channelName: channel.channelName,
      integrationId: channel.integrationId,
      status: 'mapping_missing',
    }
  }

  const { day, windowStart, windowEnd } = resolveDiffWindow(targetDay)

  const alreadySummarized = await getUnitIdsWithSummary(
    qx,
    channel.units.map((unit) => unit.id),
    day,
  )
  const pendingUnits = channel.units.filter((unit) => !alreadySummarized.has(unit.id))

  const unitDiffs: { unit: IShadowDiffUnit; result: IShadowDiffUnitResult }[] = []
  for (const unit of pendingUnits) {
    const result = await diffUnit(qx, unit, mapping.connectionId, windowStart, windowEnd)
    unitDiffs.push({ unit, result })
  }

  await qx.tx(async (txQx) => {
    for (const { unit, result } of unitDiffs) {
      await persistUnitDiffResult(txQx, channel, unit, day, windowStart, windowEnd, result)
    }
  })

  return {
    channelName: channel.channelName,
    integrationId: channel.integrationId,
    status: 'ok',
  }
}
