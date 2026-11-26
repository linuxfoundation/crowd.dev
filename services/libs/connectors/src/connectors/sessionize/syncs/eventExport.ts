import { z } from 'zod'

import { timeout } from '@crowd/common'

import type { EmitOverrides } from '../../../emit'
import { ConnectorCodeError } from '../../../http/errors'
import type { SyncContext, SyncOutcome } from '../../../types'
import type { SessionizeDeps } from '../deps'
import {
  SESSIONIZE_ACCEPTED_SESSIONS_PATH,
  SESSIONIZE_ALL_SESSIONS_PATH,
  downloadSessionizeExport,
  readExportSheet,
} from '../exportFile'
import { compareWithLastGoodRun, parseSessionizeExport } from '../exportRows'
import type { ExportProfile, ExportVariant, ParsedExport, SessionizeExportRow } from '../exportRows'
import { spokeAtSessionActivity, submittedSessionActivity } from '../mapper'
import type { SessionizeActivity, SessionizeEventContext } from '../mapper'

export const SESSIONIZE_DOWNLOAD_GAP_MS = 5_000
export const EMIT_CHUNK_SIZE = 200

const DAY_MS = 24 * 60 * 60 * 1000
const ENDED_AFTER_DAYS = 2
const DAILY_UNTIL_DAYS = 90
const WEEKLY_UNTIL_DAYS = 365
const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/

const EXPORT_PATHS: Record<ExportVariant, (eventKey: string) => string> = {
  all: SESSIONIZE_ALL_SESSIONS_PATH,
  accepted: SESSIONIZE_ACCEPTED_SESSIONS_PATH,
}

const exportProfileSchema = z.object({
  columns: z.array(z.string()),
  rowCount: z.number(),
  emailFillRate: z.number(),
  linkedinFillRate: z.number(),
})

type EventExportWatermark = {
  loadedOnce: boolean
  spokeAtEmitted: boolean
  eventEnd: string
  finished?: true
  lastGoodRun: { all: ExportProfile; accepted?: ExportProfile }
}

const watermarkSchema = z.object({
  loadedOnce: z.boolean(),
  spokeAtEmitted: z.boolean(),
  eventEnd: z.string(),
  finished: z.literal(true).optional(),
  lastGoodRun: z.object({
    all: exportProfileSchema,
    accepted: exportProfileSchema.optional(),
  }),
})

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

export function nextEventExportRun(
  eventEnd: string,
  now: Date,
): { nextRunAt: Date } | { finished: true } {
  const daysSinceEnd = (now.getTime() - eventEndMs(eventEnd)) / DAY_MS
  if (daysSinceEnd >= WEEKLY_UNTIL_DAYS) {
    return { finished: true }
  }
  const gapDays = daysSinceEnd >= DAILY_UNTIL_DAYS ? 7 : 1
  return { nextRunAt: new Date(now.getTime() + gapDays * DAY_MS) }
}

function readWatermark(raw: Record<string, unknown> | null): EventExportWatermark | null {
  if (raw === null) {
    return null
  }
  const parsed = watermarkSchema.safeParse(raw)
  if (!parsed.success) {
    throw new ConnectorCodeError('sessionize eventExport watermark is malformed', {
      cause: parsed.error.issues.map((issue) => issue.path.join('.')),
    })
  }
  return parsed.data as EventExportWatermark
}

async function loadExport(
  ctx: SyncContext<SessionizeDeps>,
  eventKey: string,
  variant: ExportVariant,
  lastGoodRun: ExportProfile | null,
): Promise<ParsedExport> {
  await timeout(SESSIONIZE_DOWNLOAD_GAP_MS)
  const file = await downloadSessionizeExport(ctx.http, EXPORT_PATHS[variant](eventKey))
  const parsed = parseSessionizeExport(await readExportSheet(file), variant)
  for (const { kind, detail } of compareWithLastGoodRun(parsed, lastGoodRun)) {
    ctx.log.warn(
      { event: 'sessionize_export_warning', eventKey, variant, kind, detail },
      'sessionize export needs a look',
    )
  }
  return parsed
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

function eventActivities(
  ctx: SyncContext<SessionizeDeps>,
  event: SessionizeEventContext,
  all: ParsedExport,
  accepted: ParsedExport | null,
): SessionizeActivity[] {
  const submitted = mapRows(all.rows, (row) => submittedSessionActivity(row, event))
  logSkipped(ctx, event.eventKey, 'all', {
    ...all.skipped,
    withoutSubmissionDate: submitted.unmapped,
  })
  if (accepted === null) {
    return submitted.activities
  }
  const spoke = mapRows(accepted.rows, (row) => spokeAtSessionActivity(row, event))
  logSkipped(ctx, event.eventKey, 'accepted', { ...accepted.skipped, notAccepted: spoke.unmapped })
  return [...submitted.activities, ...spoke.activities]
}

function nextWatermark(
  previous: EventExportWatermark | null,
  event: SessionizeEventContext,
  ended: boolean,
  finished: boolean,
  all: ParsedExport,
  accepted: ParsedExport | null,
): EventExportWatermark {
  const acceptedProfile = accepted?.profile ?? previous?.lastGoodRun.accepted
  return {
    loadedOnce: true,
    spokeAtEmitted: (previous?.spokeAtEmitted ?? false) || ended,
    eventEnd: event.eventEnd,
    ...(finished && { finished: true as const }),
    lastGoodRun: { all: all.profile, ...(acceptedProfile && { accepted: acceptedProfile }) },
  }
}

export async function runEventExport(ctx: SyncContext<SessionizeDeps>): Promise<SyncOutcome> {
  const eventKey = ctx.channel.channelId
  const event = await ctx.deps.getEvent(eventKey)
  if (event === null) {
    throw new ConnectorCodeError(`sessionize event ${eventKey} has no mapping row`)
  }
  const previous = readWatermark(ctx.watermark)
  const now = new Date()
  const ended = hasEventEnded(event.eventEnd, now)

  const all = await loadExport(ctx, eventKey, 'all', previous?.lastGoodRun.all ?? null)
  const accepted = ended
    ? await loadExport(ctx, eventKey, 'accepted', previous?.lastGoodRun.accepted ?? null)
    : null
  const activities = eventActivities(ctx, event, all, accepted)

  const overrides: EmitOverrides | undefined =
    ended && !previous?.loadedOnce ? { backfill: true } : undefined
  for (let start = 0; start < activities.length; start += EMIT_CHUNK_SIZE) {
    if (!ctx.hasRunBudget()) {
      ctx.log.info(
        {
          event: 'sessionize_export_out_of_budget',
          eventKey,
          emitted: start,
          total: activities.length,
        },
        'sessionize export run stopped for lack of budget, the next run loads the event again',
      )
      return { complete: false }
    }
    await ctx.emit(activities.slice(start, start + EMIT_CHUNK_SIZE), overrides)
  }

  const next = nextEventExportRun(event.eventEnd, now)
  const finished = 'finished' in next
  await ctx.commitWatermark(nextWatermark(previous, event, ended, finished, all, accepted))
  ctx.log.info(
    { event: 'sessionize_export_loaded', eventKey, ended, emitted: activities.length, finished },
    'sessionize event export loaded',
  )
  return finished
    ? { complete: true, finished: true }
    : { complete: true, nextRunAt: next.nextRunAt }
}
