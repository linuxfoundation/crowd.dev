import { z } from 'zod'

import { timeout } from '@crowd/common'

import type { EmitOverrides } from '../../../emit'
import { ConnectorCodeError, ProviderContractError } from '../../../http/errors'
import type { SyncContext, SyncOutcome } from '../../../types'
import type { SessionizeDeps } from '../deps'
import {
  SESSIONIZE_ACCEPTED_SESSIONS_PATH,
  SESSIONIZE_ALL_SESSIONS_PATH,
  downloadSessionizeExport,
  readExportSheet,
} from '../exportFile'
import { compareWithLastGoodRun, exportProfileSchema, parseSessionizeExport } from '../exportRows'
import type {
  ExportProfile,
  ExportVariant,
  ExportWarning,
  ParsedExport,
  SessionizeExportRow,
} from '../exportRows'
import { spokeAtSessionActivity, submittedSessionActivity } from '../mapper'
import type { SessionizeActivity, SessionizeEventContext } from '../mapper'

export const SESSIONIZE_DOWNLOAD_GAP_MS = 5_000
export const EMIT_CHUNK_SIZE = 200

const DAY_MS = 24 * 60 * 60 * 1000
const ENDED_AFTER_DAYS = 2
const CHECKPOINT_MARGIN_MS = DAY_MS
const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/
const EVENT_KEY = /^\d+$/

const EXPORT_PATHS: Record<ExportVariant, (eventKey: string) => string> = {
  all: SESSIONIZE_ALL_SESSIONS_PATH,
  accepted: SESSIONIZE_ACCEPTED_SESSIONS_PATH,
}

const watermarkSchema = z.object({
  lastRunStartedAt: z.string().datetime(),
  finished: z.literal(true).optional(),
  lastGoodRun: z.object({
    all: exportProfileSchema,
    accepted: exportProfileSchema.optional(),
  }),
})

type EventExportWatermark = z.infer<typeof watermarkSchema>

function eventEndMs(eventEnd: string): number {
  const ms = ISO_DATE.test(eventEnd) ? Date.parse(`${eventEnd}T00:00:00.000Z`) : NaN
  if (Number.isNaN(ms)) {
    throw new ConnectorCodeError(`sessionize event end "${eventEnd}" is not a date`)
  }
  return ms
}

export function hasEventEnded(eventEnd: string, now: Date): boolean {
  return now.getTime() >= eventEndMs(eventEnd) + ENDED_AFTER_DAYS * DAY_MS
}

function assertEventKey(eventKey: string): void {
  if (!EVENT_KEY.test(eventKey)) {
    throw new ConnectorCodeError(`sessionize event key "${eventKey}" is not numeric`)
  }
}

function readWatermark(raw: Record<string, unknown> | null): EventExportWatermark | null {
  if (raw === null || Object.keys(raw).length === 0) {
    return null
  }
  const parsed = watermarkSchema.safeParse(raw)
  if (!parsed.success) {
    throw new ConnectorCodeError('sessionize eventExport watermark is malformed', {
      cause: parsed.error.issues.map((issue) => issue.path.join('.')),
    })
  }
  return parsed.data
}

async function loadExport(
  ctx: SyncContext<SessionizeDeps>,
  eventKey: string,
  variant: ExportVariant,
  lastGoodRun: ExportProfile | null,
): Promise<{ parsed: ParsedExport; warnings: ExportWarning[] }> {
  await timeout(SESSIONIZE_DOWNLOAD_GAP_MS)
  const file = await downloadSessionizeExport(ctx.http, EXPORT_PATHS[variant](eventKey))
  const parsed = parseSessionizeExport(await readExportSheet(file), variant)
  const warnings = compareWithLastGoodRun(parsed, lastGoodRun)
  for (const { kind, detail } of warnings) {
    ctx.log.warn(
      { event: 'sessionize_export_warning', eventKey, variant, kind, detail },
      'sessionize export needs a look',
    )
  }
  return { parsed, warnings }
}

function isRowCountDrop({ kind }: ExportWarning): boolean {
  return kind === 'row_count_drop'
}

function assertFinalLoadComplete(
  eventKey: string,
  all: ParsedExport,
  allWarnings: ExportWarning[],
  accepted: ParsedExport,
): void {
  const rowCountDrop = allWarnings.find(isRowCountDrop)
  if (rowCountDrop) {
    throw new ProviderContractError(
      `sessionize event ${eventKey} has ended but its all sessions export lost more than half of its rows`,
      { cause: rowCountDrop.detail },
    )
  }
  const acceptedInAll = all.rows.filter(({ status }) => status === 'Accepted').length
  if (acceptedInAll > 0 && accepted.rows.length === 0) {
    throw new ProviderContractError(
      `sessionize event ${eventKey} has ended with ${acceptedInAll} accepted rows but an empty accepted export`,
    )
  }
}

function logSkipped(
  ctx: SyncContext<SessionizeDeps>,
  eventKey: string,
  variant: ExportVariant,
  counts: Record<string, number>,
): void {
  if (Object.values(counts).some((count) => count > 0)) {
    ctx.log.info(
      { event: 'sessionize_export_skipped', eventKey, variant, ...counts },
      'sessionize export rows skipped',
    )
  }
}

function mapRows(
  rows: SessionizeExportRow[],
  toActivity: (row: SessionizeExportRow) => SessionizeActivity | null,
): { activities: SessionizeActivity[]; unmapped: number } {
  const activities = rows.map(toActivity).filter((activity) => activity !== null)
  return { activities, unmapped: rows.length - activities.length }
}

function submittedSince(activities: SessionizeActivity[], since: number | null) {
  return since === null
    ? activities
    : activities.filter((activity) => Date.parse(activity.timestamp) >= since)
}

function eventActivities(
  ctx: SyncContext<SessionizeDeps>,
  event: SessionizeEventContext,
  all: ParsedExport,
  accepted: ParsedExport | null,
  since: number | null,
): SessionizeActivity[] {
  const submitted = mapRows(all.rows, (row) => submittedSessionActivity(row, event))
  logSkipped(ctx, event.eventKey, 'all', {
    ...all.skipped,
    withoutSubmissionDate: submitted.unmapped,
  })
  const sent = submittedSince(submitted.activities, since)
  if (accepted === null) {
    return sent
  }
  const spoke = mapRows(accepted.rows, (row) => spokeAtSessionActivity(row, event))
  logSkipped(ctx, event.eventKey, 'accepted', { ...accepted.skipped, notAccepted: spoke.unmapped })
  return [...sent, ...spoke.activities]
}

function checkpointSince(previous: EventExportWatermark | null, ended: boolean): number | null {
  return ended || previous === null
    ? null
    : Date.parse(previous.lastRunStartedAt) - CHECKPOINT_MARGIN_MS
}

function nextWatermark(
  previous: EventExportWatermark | null,
  startedAt: Date,
  ended: boolean,
  all: ParsedExport,
  allWarnings: ExportWarning[],
  accepted: ParsedExport | null,
): EventExportWatermark {
  const acceptedProfile = accepted?.profile ?? previous?.lastGoodRun.accepted
  const allProfile =
    previous !== null && allWarnings.some(isRowCountDrop) ? previous.lastGoodRun.all : all.profile
  return {
    lastRunStartedAt: startedAt.toISOString(),
    ...(ended && { finished: true as const }),
    lastGoodRun: { all: allProfile, ...(acceptedProfile && { accepted: acceptedProfile }) },
  }
}

export async function runEventExport(ctx: SyncContext<SessionizeDeps>): Promise<SyncOutcome> {
  const eventKey = ctx.channel.channelId
  assertEventKey(eventKey)
  const event = await ctx.deps.getEvent(eventKey)
  if (event === null) {
    throw new ConnectorCodeError(`sessionize event ${eventKey} has no mapping row`)
  }
  const previous = readWatermark(ctx.watermark)
  const startedAt = new Date()
  const ended = hasEventEnded(event.eventEnd, startedAt)
  const since = checkpointSince(previous, ended)

  const { parsed: all, warnings: allWarnings } = await loadExport(
    ctx,
    eventKey,
    'all',
    previous?.lastGoodRun.all ?? null,
  )
  const accepted = ended
    ? (await loadExport(ctx, eventKey, 'accepted', previous?.lastGoodRun.accepted ?? null)).parsed
    : null
  if (accepted !== null) {
    assertFinalLoadComplete(eventKey, all, allWarnings, accepted)
  }
  const activities = eventActivities(ctx, event, all, accepted, since)

  const overrides: EmitOverrides | undefined = since === null ? { backfill: true } : undefined
  for (let start = 0; start < activities.length; start += EMIT_CHUNK_SIZE) {
    if (!ctx.hasRunBudget()) {
      ctx.log.info(
        {
          event: 'sessionize_export_out_of_budget',
          eventKey,
          emitted: start,
          total: activities.length,
        },
        'sessionize export run stopped for lack of budget, the next run repeats the same load',
      )
      return { complete: false }
    }
    await ctx.emit(activities.slice(start, start + EMIT_CHUNK_SIZE), overrides)
  }

  await ctx.commitWatermark(nextWatermark(previous, startedAt, ended, all, allWarnings, accepted))
  ctx.log.info(
    {
      event: 'sessionize_export_loaded',
      eventKey,
      fullLoad: since === null,
      ended,
      emitted: activities.length,
    },
    'sessionize event export loaded',
  )
  return ended
    ? { complete: true, finished: true }
    : { complete: true, nextRunAt: new Date(startedAt.getTime() + DAY_MS) }
}
