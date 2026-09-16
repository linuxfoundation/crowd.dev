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
  NangoMetadataLastAction,
  getNangoCloudRecords,
  initNangoCloudClient,
} from '@crowd/nango'
import { SlackChannel, SlackPersona, sendSlackNotificationAsync } from '@crowd/slack'

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
const MAX_REPORTED_MISMATCHES = 50
const MAX_DETAILED_MISMATCHES_PER_SYNC = 5
const REPORTED_COUNT_KINDS: readonly ShadowDiffMismatchKind[] = [
  'missing_in_nango',
  'missing_in_shadow',
  'field_mismatch',
]
const SLACK_HEADER_MAX_LENGTH = 150
const SLACK_ICON_PREFIX_MAX_LENGTH = 18 // longest persona icon used below, ':rotating_light: '
const SLACK_TITLE_MAX_LENGTH = SLACK_HEADER_MAX_LENGTH - SLACK_ICON_PREFIX_MAX_LENGTH

export interface IShadowDiffChannel {
  channelName: string
  integrationId: string
  units: IShadowDiffUnit[]
}

export type ShadowDiffChannelStatus = 'ok' | 'mapping_missing' | 'error'

export interface IShadowDiffSyncSummary {
  syncName: string
  counts: Record<ShadowDiffMismatchKind, number>
}

export interface IShadowDiffChannelResult {
  channelName: string
  integrationId: string
  status: ShadowDiffChannelStatus
  mismatches: IShadowDiffMismatch[]
  totalMismatchCount: number
  syncSummaries: IShadowDiffSyncSummary[]
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

function diffableRecordKey(record: IDiffableRecord): string {
  return `${record.type}::${record.sourceId}`
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
    return [
      {
        sourceId: unit.id,
        type: unit.syncName,
        kind: 'unsupported_sync',
        severity: 'high',
        syncName: unit.syncName,
      },
    ]
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

  const mismatches = diffShadowAgainstNango(
    shadowRecords
      .map(toDiffableShadowRecord)
      .filter((r) => !deletedNangoKeys.has(diffableRecordKey(r))),
    diffableNangoRecords.filter((r) => !deletedNangoKeys.has(diffableRecordKey(r))),
  )

  return mismatches.map((mismatch) => ({ ...mismatch, syncName: unit.syncName }))
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

export async function runShadowDiffForChannel(
  channel: IShadowDiffChannel,
): Promise<IShadowDiffChannelResult> {
  const qx = dbStoreQx(svc.postgres.writer)

  const { owner, repo } = parseRepoChannel(channel.channelName)
  const mapping = await getNangoMappingForRepo(qx, channel.integrationId, owner, repo)

  if (!mapping) {
    return {
      channelName: channel.channelName,
      integrationId: channel.integrationId,
      status: 'mapping_missing',
      mismatches: [],
      totalMismatchCount: 0,
      syncSummaries: [],
    }
  }

  const { windowStart, windowEnd } = previousDayWindow(new Date(Date.now() - MS_PER_DAY))
  const mismatches: IShadowDiffMismatch[] = []
  const syncSummaries: IShadowDiffSyncSummary[] = []
  let totalMismatchCount = 0

  for (const unit of channel.units) {
    const unitMismatches = await diffUnit(qx, unit, mapping.connectionId, windowStart, windowEnd)
    totalMismatchCount += unitMismatches.length
    syncSummaries.push({ syncName: unit.syncName, counts: countMismatchesByKind(unitMismatches) })
    if (mismatches.length < MAX_REPORTED_MISMATCHES) {
      mismatches.push(...unitMismatches.slice(0, MAX_REPORTED_MISMATCHES - mismatches.length))
    }
  }

  return {
    channelName: channel.channelName,
    integrationId: channel.integrationId,
    status: 'ok',
    mismatches,
    totalMismatchCount,
    syncSummaries,
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

function truncateSlackTitle(title: string): string {
  if (title.length <= SLACK_TITLE_MAX_LENGTH) {
    return title
  }
  return `${title.slice(0, SLACK_TITLE_MAX_LENGTH - 1)}…`
}

function formatSyncSummaryTable(summaries: IShadowDiffSyncSummary[]): string {
  const nameWidth = Math.max('syncName'.length, ...summaries.map((s) => s.syncName.length))
  const header = `${'syncName'.padEnd(nameWidth)}  ${REPORTED_COUNT_KINDS.join('  ')}`
  const rows = summaries.map((summary) => {
    const cells = REPORTED_COUNT_KINDS.map((kind) =>
      String(summary.counts[kind]).padEnd(kind.length),
    )
    return `${summary.syncName.padEnd(nameWidth)}  ${cells.join('  ')}`
  })
  return ['```', header, ...rows, '```'].join('\n')
}

function formatUnsupportedSyncNotes(summaries: IShadowDiffSyncSummary[]): string[] {
  return summaries
    .filter((summary) => summary.counts.unsupported_sync > 0)
    .map((summary) => `- ${summary.syncName}: no Nango model mapping (unsupported_sync)`)
}

function formatSyncDetails(
  mismatches: IShadowDiffMismatch[],
  syncSummaries: IShadowDiffSyncSummary[],
): string[] {
  const mismatchesBySyncName = new Map<string, IShadowDiffMismatch[]>()
  for (const mismatch of mismatches) {
    const syncName = mismatch.syncName ?? 'unknown'
    const list = mismatchesBySyncName.get(syncName) ?? []
    list.push(mismatch)
    mismatchesBySyncName.set(syncName, list)
  }

  const sections: string[] = []
  for (const summary of syncSummaries) {
    const syncMismatches = mismatchesBySyncName.get(summary.syncName)
    if (!syncMismatches || syncMismatches.length === 0) {
      continue
    }
    const totalForSync = REPORTED_COUNT_KINDS.reduce((sum, kind) => sum + summary.counts[kind], 0)
    const shown = syncMismatches.slice(0, MAX_DETAILED_MISMATCHES_PER_SYNC)
    const moreNotice =
      totalForSync > shown.length
        ? `\n  … ${totalForSync - shown.length} more mismatch(es) not shown for this sync`
        : ''
    sections.push(
      `*${summary.syncName}*\n${shown.map((m) => `  ${formatMismatch(m)}`).join('\n')}${moreNotice}`,
    )
  }
  return sections
}

export async function reportShadowDiffResults(results: IShadowDiffChannelResult[]): Promise<void> {
  const failedChannels: string[] = []

  for (const result of results) {
    if (result.status === 'error') {
      const sent = await sendSlackNotificationAsync(
        SlackChannel.CDP_INTEGRATIONS_ALERTS,
        SlackPersona.ERROR_REPORTER,
        truncateSlackTitle(`Shadow diff failed for ${describeChannel(result)}`),
        result.errorMessage ?? 'unknown error',
      )
      if (!sent) {
        failedChannels.push(describeChannel(result))
      }
      continue
    }

    if (result.status === 'mapping_missing') {
      const sent = await sendSlackNotificationAsync(
        SlackChannel.CDP_INTEGRATIONS_ALERTS,
        SlackPersona.WARNING_PROPAGATOR,
        truncateSlackTitle(`Shadow diff: no nango mapping for ${describeChannel(result)}`),
        'This channel is in shadow mode but has no matching integration.nango_mapping row, so it could not be compared against nango.',
      )
      if (!sent) {
        failedChannels.push(describeChannel(result))
      }
      continue
    }

    if (result.mismatches.length > 0) {
      const truncatedNotice =
        result.totalMismatchCount > result.mismatches.length
          ? `\n… ${result.totalMismatchCount - result.mismatches.length} more mismatch(es) not shown overall`
          : ''
      const body =
        [
          formatSyncSummaryTable(result.syncSummaries),
          ...formatUnsupportedSyncNotes(result.syncSummaries),
          ...formatSyncDetails(result.mismatches, result.syncSummaries),
        ].join('\n\n') + truncatedNotice
      const sent = await sendSlackNotificationAsync(
        SlackChannel.CDP_INTEGRATIONS_ALERTS,
        SlackPersona.WARNING_PROPAGATOR,
        truncateSlackTitle(`Shadow diff mismatches for ${describeChannel(result)}`),
        body,
      )
      if (!sent) {
        failedChannels.push(describeChannel(result))
      }
    }
  }

  if (failedChannels.length > 0) {
    throw new Error(`Failed to deliver shadow diff Slack alerts for: ${failedChannels.join(', ')}`)
  }
}
