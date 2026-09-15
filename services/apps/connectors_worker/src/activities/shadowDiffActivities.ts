import { parseRepoChannel } from '@crowd/connectors/src/connectors/github/paging'
import {
  IShadowDiffUnit,
  getShadowRecordsInWindow,
  listShadowDiffUnits,
} from '@crowd/data-access-layer/src/connectors'
import { getNangoMappingForRepo } from '@crowd/data-access-layer/src/integrations'
import { dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'
import {
  INangoRecord,
  NangoIntegration,
  getNangoCloudRecords,
  initNangoCloudClient,
} from '@crowd/nango'
import { SlackChannel, SlackPersona, sendSlackNotificationAsync } from '@crowd/slack'

import { svc } from '../main'
import { getNangoModelForSync } from '../nangoModelMapping'
import { fetchNangoRecordsInWindow } from '../nangoWindowFetch'
import { IDiffableRecord, IShadowDiffMismatch, diffShadowAgainstNango } from '../shadowDiff'

const MS_PER_DAY = 24 * 60 * 60 * 1000
const MAX_REPORTED_MISMATCHES = 50

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
  mismatches: IShadowDiffMismatch[]
  totalMismatchCount: number
  errorMessage?: string
}

export function previousDayWindow(now: Date = new Date()): { windowStart: Date; windowEnd: Date } {
  const windowEnd = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()))
  const windowStart = new Date(windowEnd.getTime() - MS_PER_DAY)
  return { windowStart, windowEnd }
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

function toDiffableNangoRecord(record: INangoRecord): IDiffableRecord | null {
  const activity = record.activity as Record<string, unknown> | null | undefined
  if (!activity || typeof activity.sourceId !== 'string' || typeof activity.type !== 'string') {
    return null
  }
  return { sourceId: activity.sourceId, type: activity.type, data: activity }
}

async function diffUnit(
  qx: ReturnType<typeof dbStoreQx>,
  unit: IShadowDiffUnit,
  connectionId: string,
  windowStart: Date,
  windowEnd: Date,
): Promise<IShadowDiffMismatch[]> {
  const model = getNangoModelForSync(unit.syncName)
  if (!model) {
    return []
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

  return diffShadowAgainstNango(
    shadowRecords.map(toDiffableShadowRecord),
    nangoRecords.map(toDiffableNangoRecord).filter((r): r is IDiffableRecord => r !== null),
  )
}

export async function runShadowDiffForChannel(
  channel: IShadowDiffChannel,
): Promise<IShadowDiffChannelResult> {
  const qx = dbStoreQx(svc.postgres.writer)

  try {
    const { owner, repo } = parseRepoChannel(channel.channelName)
    const mapping = await getNangoMappingForRepo(qx, channel.integrationId, owner, repo)

    if (!mapping) {
      return {
        channelName: channel.channelName,
        integrationId: channel.integrationId,
        status: 'mapping_missing',
        mismatches: [],
        totalMismatchCount: 0,
      }
    }

    const { windowStart, windowEnd } = previousDayWindow()
    const mismatches: IShadowDiffMismatch[] = []

    for (const unit of channel.units) {
      const unitMismatches = await diffUnit(qx, unit, mapping.connectionId, windowStart, windowEnd)
      mismatches.push(...unitMismatches)
    }

    return {
      channelName: channel.channelName,
      integrationId: channel.integrationId,
      status: 'ok',
      mismatches: mismatches.slice(0, MAX_REPORTED_MISMATCHES),
      totalMismatchCount: mismatches.length,
    }
  } catch (err) {
    svc.log.error({ err, channelName: channel.channelName }, 'shadow diff failed for channel')
    return {
      channelName: channel.channelName,
      integrationId: channel.integrationId,
      status: 'error',
      mismatches: [],
      totalMismatchCount: 0,
      errorMessage: err instanceof Error ? err.message : String(err),
    }
  }
}

function formatMismatch(mismatch: IShadowDiffMismatch): string {
  if (mismatch.kind === 'field_mismatch') {
    const fields = (mismatch.fields ?? [])
      .map(
        (f) =>
          `${f.field}: shadow=${JSON.stringify(f.shadowValue)} nango=${JSON.stringify(f.nangoValue)}`,
      )
      .join(', ')
    return `[${mismatch.severity}] ${mismatch.type}/${mismatch.sourceId} field mismatch — ${fields}`
  }
  return `[${mismatch.severity}] ${mismatch.type}/${mismatch.sourceId} ${mismatch.kind}`
}

function describeChannel(result: IShadowDiffChannelResult): string {
  return `${result.channelName} (integration ${result.integrationId})`
}

export async function reportShadowDiffResults(results: IShadowDiffChannelResult[]): Promise<void> {
  for (const result of results) {
    if (result.status === 'error') {
      await sendSlackNotificationAsync(
        SlackChannel.CDP_INTEGRATIONS_ALERTS,
        SlackPersona.ERROR_REPORTER,
        `Shadow diff failed for ${describeChannel(result)}`,
        result.errorMessage ?? 'unknown error',
      )
      continue
    }

    if (result.status === 'mapping_missing') {
      await sendSlackNotificationAsync(
        SlackChannel.CDP_INTEGRATIONS_ALERTS,
        SlackPersona.WARNING_PROPAGATOR,
        `Shadow diff: no nango mapping for ${describeChannel(result)}`,
        'This channel is in shadow mode but has no matching integration.nango_mapping row, so it could not be compared against nango.',
      )
      continue
    }

    if (result.mismatches.length > 0) {
      const truncatedNotice =
        result.totalMismatchCount > result.mismatches.length
          ? `\n… ${result.totalMismatchCount - result.mismatches.length} more mismatch(es) not shown`
          : ''
      await sendSlackNotificationAsync(
        SlackChannel.CDP_INTEGRATIONS_ALERTS,
        SlackPersona.WARNING_PROPAGATOR,
        `Shadow diff mismatches for ${describeChannel(result)}`,
        result.mismatches.map(formatMismatch).join('\n') + truncatedNotice,
      )
    }
  }
}
