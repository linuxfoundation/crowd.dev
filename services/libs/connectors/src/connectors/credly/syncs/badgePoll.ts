import type { ICredlyBadgeInput, ICredlyBadgeRow } from '@crowd/data-access-layer/src/credly/badges'

import { ConnectorError, ProviderUnavailableError } from '../../../http/errors'
import { assertCredentialKind } from '../../../types'
import type { SyncContext, SyncOutcome } from '../../../types'
import { fetchBadge, fetchBadgesPage } from '../apiClient'
import type { CredlyPage } from '../apiClient'
import type { CredlyBadge } from '../badge'
import type { CredlyDeps, CredlyTemplateTarget } from '../deps'
import {
  credlyActivitySchema,
  issuedBadgeActivity,
  stateBadgeActivity,
  toIsoTimestamp,
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
// A renewed badge keeps the old state_updated_at (linuxfoundation/lfx-cdp-insights-ops#967), so no
// window ever lists it: it is reached only through the replaced badge's link, chain by chain.
export const MAX_REPLACEMENT_HOPS = 5

type Window = {
  since: string
  until: string
  page: number
}

// Walked from `top` down to `floor` once the live cursor is caught up; a window always starts at
// max(floor, top - MAX_WINDOW_MS), so `top` alone says where the next one goes.
export type CredlyBackfillCursor = {
  floor: string
  top: string
  since: string | null
  until: string | null
  page: number
}

export type CredlyBadgePollWatermark = {
  since: string
  until: string | null
  page: number
  backfill?: CredlyBackfillCursor
}

function parsableIso(value: unknown): string | null {
  return typeof value === 'string' && Number.isFinite(Date.parse(value)) ? value : null
}

function savedPage(value: unknown): number {
  return typeof value === 'number' && Number.isInteger(value) && value >= 1 ? value : 1
}

function readBackfill(raw: unknown): CredlyBackfillCursor | undefined {
  if (!raw || typeof raw !== 'object') return undefined
  const cursor = raw as Record<string, unknown>
  const floor = parsableIso(cursor.floor)
  const top = parsableIso(cursor.top)
  if (!floor || !top || wholeSecond(top) <= wholeSecond(floor)) return undefined
  const since = parsableIso(cursor.since)
  const until = parsableIso(cursor.until)
  const open =
    since !== null &&
    until !== null &&
    wholeSecond(until) > wholeSecond(since) &&
    wholeSecond(until) <= wholeSecond(top)
  return {
    floor,
    top,
    since: open ? since : null,
    until: open ? until : null,
    page: open ? savedPage(cursor.page) : 1,
  }
}

function readWatermark(
  raw: Record<string, unknown> | null,
  startedAtMs: number,
): CredlyBadgePollWatermark {
  const since =
    parsableIso(raw?.since) ?? new Date(startedAtMs - FIRST_RUN_LOOKBACK_MS).toISOString()
  const until = parsableIso(raw?.until)
  const resumable = until !== null && wholeSecond(until) > wholeSecond(since)
  const backfill = readBackfill(raw?.backfill)
  return {
    since,
    until: resumable ? until : null,
    page: resumable ? savedPage(raw?.page) : 1,
    ...(backfill && { backfill }),
  }
}

function openWindow(since: string, horizonMs: number): Window | null {
  const sinceMs = Date.parse(since)
  if (horizonMs - sinceMs < 1000) return null
  return {
    since,
    until: new Date(Math.min(sinceMs + MAX_WINDOW_MS, horizonMs)).toISOString(),
    page: 1,
  }
}

function backfillWindowStart(cursor: CredlyBackfillCursor): string {
  return new Date(
    Math.max(Date.parse(cursor.floor), Date.parse(cursor.top) - MAX_WINDOW_MS),
  ).toISOString()
}

function openBackfillWindow(cursor: CredlyBackfillCursor): CredlyBackfillCursor {
  return { ...cursor, since: backfillWindowStart(cursor), until: cursor.top, page: 1 }
}

function afterBackfillDrain(cursor: CredlyBackfillCursor): CredlyBackfillCursor | undefined {
  if (wholeSecond(cursor.until) < wholeSecond(cursor.top)) {
    return { ...cursor, since: cursor.until, until: cursor.top, page: 1 }
  }
  const top = backfillWindowStart(cursor)
  if (wholeSecond(top) <= wholeSecond(cursor.floor)) return undefined
  return { ...cursor, top, since: null, until: null, page: 1 }
}

function withBackfill(
  watermark: CredlyBadgePollWatermark,
  backfill: CredlyBackfillCursor | undefined,
): CredlyBadgePollWatermark {
  const { backfill: _dropped, ...forward } = watermark
  return backfill ? { ...forward, backfill } : forward
}

// Numbered pages over a mutable sort key skip a badge whenever an earlier one leaves the window
// mid-walk, so the lower bound follows the last badge seen and the page number only breaks ties.
function nextPage(window: Window, badges: CredlyBadge[]): Window {
  const lastSeenSecond = wholeSecond(badges[badges.length - 1]?.state_updated_at ?? '')
  if (lastSeenSecond > wholeSecond(window.since) && lastSeenSecond < wholeSecond(window.until)) {
    return { ...window, since: new Date(lastSeenSecond * 1000).toISOString(), page: 1 }
  }
  return { ...window, page: window.page + 1 }
}

function wholeSecond(iso: string): number {
  return Math.floor(Date.parse(iso) / 1000)
}

function halveWindow(window: Window): Window | null {
  const sinceMs = Date.parse(window.since)
  const untilMs = Date.parse(window.until)
  const halfMs = Math.floor((untilMs - sinceMs) / 2)
  if (halfMs < MIN_WINDOW_MS) return null
  return { since: window.since, until: new Date(sinceMs + halfMs).toISOString(), page: 1 }
}

// A timeout or an unreachable host is also provider.unavailable, but halving does not help there.
function isCredlyServerError(err: unknown): boolean {
  return err instanceof ProviderUnavailableError && (err.options?.status ?? 0) >= 500
}

function isCredlyNotFound(err: unknown): boolean {
  return err instanceof ConnectorError && err.options?.status === 404
}

function newBadgeActivities(badge: CredlyBadge): CredlyActivity[] {
  const activities = [issuedBadgeActivity(badge)]
  const state = stateBadgeActivity(badge)
  if (state) activities.push(state)
  return activities.map((activity) => credlyActivitySchema.parse(activity))
}

function stateChangeActivities(badge: CredlyBadge): CredlyActivity[] {
  const state = stateBadgeActivity(badge)
  return state ? [credlyActivitySchema.parse(state)] : []
}

function badgeRow(badge: CredlyBadge, target: CredlyTemplateTarget): ICredlyBadgeInput {
  return {
    badgeId: badge.id,
    badgeTemplateId: badge.badge_template.id,
    badgeTemplateName: badge.badge_template.name,
    badgeTemplateUrl: badge.badge_template.url ?? null,
    state: badge.state,
    stateUpdatedAt: badge.state_updated_at ? toIsoTimestamp(badge.state_updated_at) : null,
    issuedAt: toIsoTimestamp(badge.issued_at),
    expiresAt: badge.expires_at ? toIsoTimestamp(badge.expires_at) : null,
    replacementBadgeId: badge.replacement_badge_id ?? null,
    replacesBadgeId: badge.replaces_badge_id ?? null,
    segmentId: target.segmentId,
    integrationId: target.integrationId,
    recipientEmail: badge.recipient_email.trim().toLowerCase(),
    issuedTo: badge.issued_to ?? null,
  }
}

function storedTarget(row: ICredlyBadgeRow): CredlyTemplateTarget {
  return { segmentId: row.segmentId, integrationId: row.integrationId, locked: true }
}

function withFirstSight(badge: CredlyBadge, stored: ICredlyBadgeRow): CredlyBadge {
  return { ...badge, recipient_email: stored.recipientEmail, issued_to: stored.issuedTo }
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
  if (ctx.watermark?.backfill && !watermark.backfill) {
    ctx.log.warn({ backfill: ctx.watermark.backfill }, 'credly backfill cursor unreadable, dropped')
  }

  const counters = {
    pagesFetched: 0,
    badges: 0,
    routed: 0,
    held: 0,
    stateChanges: 0,
    unchanged: 0,
    pendingStored: 0,
    activities: 0,
    skipped: 0,
    locked: 0,
    windows: 0,
    splits: 0,
    replacements: 0,
    replacementsMissing: 0,
    backfillPages: 0,
    backfillWindows: 0,
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

  // The table is read and written only when the emit is real: a shadow run must not make badges
  // look already emitted. A known badge keeps the member and segment of its first sight.
  const emitPage = async (badges: CredlyBadge[]) => {
    const lastByBadgeId = new Map(badges.map((badge) => [badge.id, badge]))
    const routed = [...lastByBadgeId.values()].filter((badge) =>
      routing.targets.has(badge.badge_template.id),
    )
    counters.held += lastByBadgeId.size - routed.length
    const known = new Map<string, ICredlyBadgeRow>()
    if (ctx.emitEnabled && routed.length > 0) {
      for (const row of await ctx.deps.findBadges(routed.map((badge) => badge.id))) {
        known.set(row.badgeId, row)
      }
    }

    const groups = new Map<
      string,
      {
        target: CredlyTemplateTarget
        templateIds: Set<string>
        activities: CredlyActivity[]
        rows: ICredlyBadgeInput[]
      }
    >()
    for (const badge of routed) {
      const stored = known.get(badge.id)
      const target = stored ? storedTarget(stored) : routing.targets.get(badge.badge_template.id)!
      const seen = stored ? withFirstSight(badge, stored) : badge
      let activities: CredlyActivity[]
      let row: ICredlyBadgeInput
      try {
        row = badgeRow(seen, target)
        if (!stored || stored.state === 'pending') {
          activities = seen.state === 'pending' ? [] : newBadgeActivities(seen)
        } else {
          activities = stored.state === seen.state ? [] : stateChangeActivities(seen)
        }
      } catch (err) {
        counters.skipped += 1
        ctx.log.warn(
          { badgeId: badge.id, errMsg: (err as Error).message },
          'skipping credly badge: failed to map (malformed provider data)',
        )
        continue
      }
      if (stored) {
        if (activities.length > 0) counters.stateChanges += 1
        else counters.unchanged += 1
      } else if (activities.length > 0) counters.routed += 1
      else counters.pendingStored += 1

      const key = `${target.integrationId}:${target.segmentId}`
      const group = groups.get(key) ?? {
        target,
        templateIds: new Set<string>(),
        activities: [],
        rows: [],
      }
      group.activities.push(...activities)
      group.templateIds.add(badge.badge_template.id)
      group.rows.push(row)
      groups.set(key, group)
      counters.activities += activities.length
    }
    for (const { target, templateIds, activities, rows } of groups.values()) {
      await lockBeforeEmit(templateIds, target)
      if (activities.length > 0) await ctx.emit(activities, target)
      if (ctx.emitEnabled) await ctx.deps.saveBadges(rows)
    }
    counters.badges += badges.length
  }

  const unseen = (ids: (string | null | undefined)[], seen: Set<string>): string[] => [
    ...new Set(ids.filter((id): id is string => !!id && !seen.has(id))),
  ]

  const replacementIds = (badges: CredlyBadge[], seen: Set<string>): string[] =>
    unseen(
      badges
        .filter((badge) => routing.targets.has(badge.badge_template.id))
        .map((badge) => badge.replacement_badge_id),
      seen,
    )

  // A known replacement is not fetched again, but its stored link is still followed: a run that
  // died after saving part of a chain must reach the end of the chain on retry.
  const splitKnown = async (ids: string[]): Promise<{ unknown: string[]; linked: string[] }> => {
    if (!ctx.emitEnabled || ids.length === 0) return { unknown: ids, linked: [] }
    const known = await ctx.deps.findBadges(ids)
    const knownIds = new Set(known.map((row) => row.badgeId))
    return {
      unknown: ids.filter((id) => !knownIds.has(id)),
      linked: known.map((row) => row.replacementBadgeId).filter((id): id is string => !!id),
    }
  }

  // Replacements are fetched before the page is emitted, and only those the state table does not
  // know: a run cut short re-fetches the page and finds the renewed badges still unknown.
  const withReplacements = async (badges: CredlyBadge[]): Promise<CredlyBadge[] | null> => {
    const all = [...badges]
    const seen = new Set(badges.map((badge) => badge.id))
    let ids = replacementIds(badges, seen)
    for (let hop = 0; hop < MAX_REPLACEMENT_HOPS && ids.length > 0; hop += 1) {
      for (const id of ids) seen.add(id)
      const { unknown, linked } = await splitKnown(ids)
      const fetched: CredlyBadge[] = []
      for (const badgeId of unknown) {
        if (!ctx.hasRunBudget()) return null
        try {
          fetched.push(await fetchBadge(ctx.http, { organizationId, badgeId }, ctx.log))
          counters.replacements += 1
        } catch (err) {
          if (!isCredlyNotFound(err)) throw err
          counters.replacementsMissing += 1
          ctx.log.warn({ badgeId }, 'credly replacement badge not found, skipping it')
        }
      }
      all.push(...fetched)
      ids = unseen([...replacementIds(fetched, seen), ...linked], seen)
    }
    if (ids.length > 0) {
      ctx.log.warn({ badgeIds: ids }, 'credly replacement chain cut at the hop cap')
    }
    return all
  }

  const fetchOrHalve = async (
    window: Window,
  ): Promise<{ page: CredlyPage<CredlyBadge> } | { halved: Window }> => {
    try {
      const page = await fetchBadgesPage(
        ctx.http,
        { organizationId, window: { min: window.since, max: window.until }, page: window.page },
        ctx.log,
      )
      counters.pagesFetched += 1
      return { page }
    } catch (err) {
      const halved = isCredlyServerError(err) ? halveWindow(window) : null
      if (!halved) throw err
      counters.splits += 1
      ctx.log.warn(
        { since: window.since, until: window.until, halvedUntil: halved.until },
        'credly badge window failed, retrying its first half',
      )
      return { halved }
    }
  }

  const drainedPage = (window: Window, page: CredlyPage<CredlyBadge>) =>
    window.page >= page.metadata.total_pages

  while (counters.pagesFetched < MAX_PAGES_PER_RUN && ctx.hasRunBudget()) {
    if (!watermark.until) {
      const next = openWindow(watermark.since, horizonMs)
      if (!next) break
      watermark = { ...watermark, ...next }
      await ctx.commitWatermark(watermark)
    }
    const window: Window = { since: watermark.since, until: watermark.until, page: watermark.page }

    const result = await fetchOrHalve(window)
    if ('halved' in result) {
      watermark = { ...watermark, ...result.halved }
      await ctx.commitWatermark(watermark)
      continue
    }

    const badges = await withReplacements(result.page.data)
    if (!badges) break
    await emitPage(badges)

    if (drainedPage(window, result.page)) {
      counters.windows += 1
      watermark = { ...watermark, since: window.until, until: null, page: 1 }
    } else {
      watermark = { ...watermark, ...nextPage(window, result.page.data) }
    }
    await ctx.commitWatermark(watermark)
  }

  while (
    watermark.backfill &&
    !watermark.until &&
    counters.pagesFetched < MAX_PAGES_PER_RUN &&
    ctx.hasRunBudget()
  ) {
    let cursor = watermark.backfill
    if (!cursor.since || !cursor.until) {
      cursor = openBackfillWindow(cursor)
      watermark = withBackfill(watermark, cursor)
      await ctx.commitWatermark(watermark)
    }
    const window: Window = { since: cursor.since, until: cursor.until, page: cursor.page }

    const result = await fetchOrHalve(window)
    if ('halved' in result) {
      watermark = withBackfill(watermark, { ...cursor, ...result.halved })
      await ctx.commitWatermark(watermark)
      continue
    }
    counters.backfillPages += 1

    const badges = await withReplacements(result.page.data)
    if (!badges) break
    await emitPage(badges)

    if (drainedPage(window, result.page)) {
      counters.backfillWindows += 1
      const next = afterBackfillDrain(cursor)
      if (!next) ctx.log.info({ floor: cursor.floor }, 'credly backfill reached its floor')
      watermark = withBackfill(watermark, next)
    } else {
      watermark = withBackfill(watermark, { ...cursor, ...nextPage(window, result.page.data) })
    }
    await ctx.commitWatermark(watermark)
  }

  const caughtUp = !watermark.until && !openWindow(watermark.since, horizonMs)
  ctx.log.info(
    { event: 'credly_badge_poll_run', ...counters, ...watermark, caughtUp },
    'credly badge poll run',
  )
  return { complete: caughtUp && !watermark.backfill }
}
