import { ProviderUnavailableError } from '../../../http/errors'
import { assertCredentialKind } from '../../../types'
import type { SyncContext, SyncOutcome } from '../../../types'
import { fetchBadgesPage } from '../apiClient'
import type { CredlyBadge } from '../badge'
import type { CredlyDeps, CredlyTemplateTarget } from '../deps'
import {
  credlyActivitySchema,
  expiredBadgeActivity,
  issuedBadgeActivity,
  stateBadgeActivity,
} from '../mapper'
import type { CredlyActivity } from '../mapper'

const MINUTE_MS = 60_000
const HOUR_MS = 60 * MINUTE_MS
const DAY_MS = 24 * HOUR_MS
// Seven-day windows answered in seconds in the probes of linuxfoundation/lfx-cdp-insights-ops#864
// while wider scans hit random 500s, so a window that still fails after the client's retries is halved.
export const MAX_WINDOW_MS = 7 * DAY_MS
const MIN_WINDOW_MS = HOUR_MS
// The window never reaches "now": a state change Credly stamps just before our call can land after it.
export const WINDOW_LAG_MS = 5 * MINUTE_MS
// The watermark is persisted only when the run ends, so runs stay short (~9 min at 5 s per page)
// and a long catch-up advances in persisted steps.
export const MAX_PAGES_PER_RUN = 100
const FIRST_RUN_LOOKBACK_MS = DAY_MS

export type CredlyBadgePollWatermark = {
  since: string
  until: string | null
  page: number
}

function parsableIso(value: unknown): string | null {
  return typeof value === 'string' && Number.isFinite(Date.parse(value)) ? value : null
}

function readWatermark(
  raw: Record<string, unknown> | null,
  startedAtMs: number,
): CredlyBadgePollWatermark {
  const since =
    parsableIso(raw?.since) ?? new Date(startedAtMs - FIRST_RUN_LOOKBACK_MS).toISOString()
  const until = parsableIso(raw?.until)
  const resumable = until !== null && wholeSecond(until) > wholeSecond(since)
  const page = raw?.page
  const savedPage = typeof page === 'number' && Number.isInteger(page) && page >= 1 ? page : 1
  return { since, until: resumable ? until : null, page: resumable ? savedPage : 1 }
}

function openWindow(since: string, horizonMs: number): CredlyBadgePollWatermark | null {
  const sinceMs = Date.parse(since)
  if (horizonMs - sinceMs < 1000) return null
  return {
    since,
    until: new Date(Math.min(sinceMs + MAX_WINDOW_MS, horizonMs)).toISOString(),
    page: 1,
  }
}

// Numbered pages over a mutable sort key skip a badge whenever an earlier one leaves the window
// mid-walk, so the lower bound follows the last badge seen and the page number only breaks ties.
function nextPage(
  watermark: CredlyBadgePollWatermark,
  badges: CredlyBadge[],
): CredlyBadgePollWatermark {
  const lastSeenSecond = wholeSecond(badges[badges.length - 1]?.state_updated_at ?? '')
  if (
    lastSeenSecond > wholeSecond(watermark.since) &&
    lastSeenSecond < wholeSecond(watermark.until)
  ) {
    return { ...watermark, since: new Date(lastSeenSecond * 1000).toISOString(), page: 1 }
  }
  return { ...watermark, page: watermark.page + 1 }
}

function wholeSecond(iso: string): number {
  return Math.floor(Date.parse(iso) / 1000)
}

function halveWindow(watermark: CredlyBadgePollWatermark): CredlyBadgePollWatermark | null {
  const sinceMs = Date.parse(watermark.since)
  const untilMs = Date.parse(watermark.until)
  const halfMs = Math.floor((untilMs - sinceMs) / 2)
  if (halfMs < MIN_WINDOW_MS) return null
  return { since: watermark.since, until: new Date(sinceMs + halfMs).toISOString(), page: 1 }
}

// A timeout or an unreachable host is also provider.unavailable, but halving does not help there.
function isCredlyServerError(err: unknown): boolean {
  return err instanceof ProviderUnavailableError && (err.options?.status ?? 0) >= 500
}

function badgeActivities(badge: CredlyBadge, nowMs: number): CredlyActivity[] {
  const activities = [issuedBadgeActivity(badge)]
  const state = stateBadgeActivity(badge)
  if (state) activities.push(state)
  // TODO(linuxfoundation/lfx-cdp-insights-ops#873): expiries after the last state change need the state table.
  const expired = expiredBadgeActivity(badge)
  if (expired && Date.parse(expired.timestamp) <= nowMs) activities.push(expired)
  return activities.map((activity) => credlyActivitySchema.parse(activity))
}

export async function runBadgePoll(ctx: SyncContext<CredlyDeps>): Promise<SyncOutcome> {
  const credential = await ctx.getCredential()
  assertCredentialKind(credential, 'credly-oauth-client')
  const { organizationId } = credential.data

  const routing = await ctx.deps.loadTemplateRouting()
  if (routing.staleTemplateIds.length > 0) {
    ctx.log.warn(
      { badgeTemplateIds: routing.staleTemplateIds },
      'credly template mappings point to a deleted or moved integration, their badges are held',
    )
  }

  const startedAtMs = Date.now()
  const horizonMs = startedAtMs - WINDOW_LAG_MS
  let watermark = readWatermark(ctx.watermark, startedAtMs)
  if (!ctx.watermark) {
    await ctx.commitWatermark(watermark)
  }

  const counters = {
    pagesFetched: 0,
    badges: 0,
    routed: 0,
    held: 0,
    activities: 0,
    skipped: 0,
    locked: 0,
    windows: 0,
    splits: 0,
  }
  const lockedThisRun = new Set<string>()

  // A template is locked before its first real emission: the segment is part of the activity
  // dedup key, so once badges may exist there the mapping must not move. A shadow run emits nothing.
  const lockBeforeEmit = async (templateIds: Set<string>, target: CredlyTemplateTarget) => {
    if (!ctx.emitEnabled) return
    for (const templateId of templateIds) {
      if (lockedThisRun.has(templateId) || routing.targets.get(templateId)?.locked) continue
      lockedThisRun.add(templateId)
      try {
        if (await ctx.deps.lockTemplate(templateId, target)) counters.locked += 1
        else ctx.log.warn({ badgeTemplateId: templateId }, 'credly template moved during the run')
      } catch (err) {
        ctx.log.warn({ badgeTemplateId: templateId, err }, 'credly template lock failed')
      }
    }
  }

  const emitPage = async (badges: CredlyBadge[]) => {
    const byTarget = new Map<
      string,
      { target: CredlyTemplateTarget; templateIds: Set<string>; activities: CredlyActivity[] }
    >()
    for (const badge of badges) {
      const target = routing.targets.get(badge.badge_template.id)
      if (!target) {
        counters.held += 1
        continue
      }
      let activities: CredlyActivity[]
      try {
        activities = badgeActivities(badge, startedAtMs)
      } catch (err) {
        counters.skipped += 1
        ctx.log.warn(
          { badgeId: badge.id, errMsg: (err as Error).message },
          'skipping credly badge: failed to map (malformed provider data)',
        )
        continue
      }
      const key = `${target.integrationId}:${target.segmentId}`
      const group = byTarget.get(key) ?? { target, templateIds: new Set<string>(), activities: [] }
      group.activities.push(...activities)
      group.templateIds.add(badge.badge_template.id)
      byTarget.set(key, group)
      counters.activities += activities.length
      counters.routed += 1
    }
    for (const { target, templateIds, activities } of byTarget.values()) {
      await lockBeforeEmit(templateIds, target)
      await ctx.emit(activities, target)
    }
    counters.badges += badges.length
  }

  while (counters.pagesFetched < MAX_PAGES_PER_RUN && ctx.hasRunBudget()) {
    if (!watermark.until) {
      const next = openWindow(watermark.since, horizonMs)
      if (!next) break
      watermark = next
      await ctx.commitWatermark(watermark)
    }

    let page
    try {
      page = await fetchBadgesPage(
        ctx.http,
        {
          organizationId,
          window: { min: watermark.since, max: watermark.until },
          page: watermark.page,
        },
        ctx.log,
      )
    } catch (err) {
      const halved = isCredlyServerError(err) ? halveWindow(watermark) : null
      if (!halved) throw err
      counters.splits += 1
      ctx.log.warn(
        { since: watermark.since, until: watermark.until, halvedUntil: halved.until },
        'credly badge window failed, retrying its first half',
      )
      watermark = halved
      await ctx.commitWatermark(watermark)
      continue
    }
    counters.pagesFetched += 1

    await emitPage(page.data)

    const drained = watermark.page >= page.metadata.total_pages
    if (drained) {
      counters.windows += 1
      watermark = { since: watermark.until, until: null, page: 1 }
    } else {
      watermark = nextPage(watermark, page.data)
    }
    await ctx.commitWatermark(watermark)
  }

  const caughtUp = !watermark.until && !openWindow(watermark.since, horizonMs)
  ctx.log.info(
    { event: 'credly_badge_poll_run', ...counters, ...watermark, caughtUp },
    'credly badge poll run',
  )
  return caughtUp ? { complete: true } : { complete: false }
}
