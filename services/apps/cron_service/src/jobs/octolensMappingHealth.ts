import type {
  IDeactivatedOctolensKeywordMapping,
  IOctolensMappingDiscoveryState,
} from '@crowd/data-access-layer'
import { SlackMessageSection, SlackPersona, escapeSlackText } from '@crowd/slack'

export const STALE_AFTER_HOURS = 3
export const STALE_REMINDER_HOURS = 24
export const BURST_MIN_DEACTIVATIONS = 3
export const BURST_MIN_SHARE = 0.25
export const CURSOR_SAFETY_MARGIN_MINUTES = 5
export const INITIAL_LOOKBACK_HOURS = 24
export const MAX_ROWS_PER_SECTION = 25

export const DEACTIVATIONS_CURSOR_KEY = 'octolens-mapping-health:deactivations-reported-until'

export function staleAlertedKey(integrationId: string): string {
  return `octolens-mapping-health:stale-alerted:${integrationId}`
}

const HOUR_MS = 60 * 60 * 1000
const MINUTE_MS = 60 * 1000

export interface IOctolensMappingHealthAlert {
  persona: SlackPersona
  title: string
  sections: SlackMessageSection[]
}

export interface IOctolensMappingHealthDeps {
  listStates: () => Promise<IOctolensMappingDiscoveryState[]>
  listDeactivatedBetween: (
    since: Date,
    until: Date,
  ) => Promise<IDeactivatedOctolensKeywordMapping[]>
  getKey: (key: string) => Promise<string | null>
  setKey: (key: string, value: string, expireAfterSeconds?: number) => Promise<void>
  sendAlert: (alert: IOctolensMappingHealthAlert) => Promise<boolean>
}

export interface IOctolensMappingHealthResult {
  integrations: number
  staleAlerted: number
  deactivationsReported: number
  sendFailures: number
}

export function hoursSince(from: Date, now: Date): number {
  return (now.getTime() - from.getTime()) / HOUR_MS
}

function stalenessBaseline(state: IOctolensMappingDiscoveryState): Date {
  return state.lastDiscoveredAt ?? state.integrationCreatedAt
}

export function isStale(state: IOctolensMappingDiscoveryState): boolean {
  return hoursSince(stalenessBaseline(state), state.checkedAt) > STALE_AFTER_HOURS
}

export function staleEpisodeId(state: IOctolensMappingDiscoveryState): string {
  return state.lastDiscoveredAt
    ? state.lastDiscoveredAt.toISOString()
    : `never:${state.integrationCreatedAt.toISOString()}`
}

function describeLastDiscovery(state: IOctolensMappingDiscoveryState): string {
  const baseline = stalenessBaseline(state)
  const ago = `${formatUtc(baseline)} (${Math.floor(hoursSince(baseline, state.checkedAt))}h ago)`
  return state.lastDiscoveredAt
    ? `Last successful discovery: ${ago}`
    : `Discovery has never completed a run since the integration was created at ${ago}`
}

export function isDeactivationBurst(deactivated: number, activeMappings: number): boolean {
  if (deactivated >= BURST_MIN_DEACTIVATIONS) {
    return true
  }
  const total = activeMappings + deactivated
  return total > 0 && deactivated / total >= BURST_MIN_SHARE
}

export function deactivationWindow(
  checkedAt: Date,
  cursor: string | null,
): { since: Date; until: Date; fromCursor: boolean } {
  const until = new Date(checkedAt.getTime() - CURSOR_SAFETY_MARGIN_MINUTES * MINUTE_MS)
  const cursorDate = cursor ? new Date(cursor) : null
  if (cursorDate && !Number.isNaN(cursorDate.getTime())) {
    return { since: cursorDate, until, fromCursor: true }
  }
  return {
    since: new Date(until.getTime() - INITIAL_LOOKBACK_HOURS * HOUR_MS),
    until,
    fromCursor: false,
  }
}

function formatUtc(date: Date): string {
  return `${date.toISOString().slice(0, 16).replace('T', ' ')} UTC`
}

export function buildStaleAlert(
  states: IOctolensMappingDiscoveryState[],
): IOctolensMappingHealthAlert {
  return {
    persona: SlackPersona.WARNING_PROPAGATOR,
    title: 'Octolens mapping discovery looks stale',
    sections: [
      {
        title: '',
        text:
          `Keyword-to-project mapping discovery has not completed a run in over ${STALE_AFTER_HOURS} hours. ` +
          'Existing mappings stay as they are, but keywords added, remapped or ' +
          'removed in Octolens are not picked up until discovery succeeds again.',
      },
      ...states.map((state) => ({
        title: `Integration ${state.integrationId}`,
        text: [
          describeLastDiscovery(state),
          `Active mappings: ${state.activeMappings}`,
          ...(state.activeMappings === 0
            ? ['No active mappings: every keyword is unmapped, so no mentions are being ingested.']
            : []),
        ].join('\n'),
      })),
    ],
  }
}

// Capped because @crowd/slack silently prunes oversized messages while still reporting
// success, which would move the cursor past deactivations nobody saw.
function formatDeactivationLines(rows: IDeactivatedOctolensKeywordMapping[]): string[] {
  const lines = rows
    .slice(0, MAX_ROWS_PER_SECTION)
    .map(
      (row) =>
        `• ${escapeSlackText(row.keyword)} (${row.keywordId}) → ` +
        `${escapeSlackText(row.segmentName)} (${escapeSlackText(row.segmentSlug)}), ` +
        `deactivated ${formatUtc(row.deactivatedAt)}`,
    )
  if (rows.length > MAX_ROWS_PER_SECTION) {
    lines.push(`… and ${rows.length - MAX_ROWS_PER_SECTION} more`)
  }
  return lines
}

export function buildDeactivationAlert(
  deactivations: IDeactivatedOctolensKeywordMapping[],
  states: IOctolensMappingDiscoveryState[],
  since: Date,
  until: Date,
): IOctolensMappingHealthAlert {
  const activeByIntegration = new Map(states.map((s) => [s.integrationId, s.activeMappings]))
  const byIntegration = new Map<string, IDeactivatedOctolensKeywordMapping[]>()
  for (const row of deactivations) {
    const bucket = byIntegration.get(row.integrationId) ?? []
    bucket.push(row)
    byIntegration.set(row.integrationId, bucket)
  }

  let burst = false
  const integrationSections: SlackMessageSection[] = []
  for (const [integrationId, rows] of byIntegration) {
    const active = activeByIntegration.get(integrationId) ?? 0
    const integrationBurst = isDeactivationBurst(rows.length, active)
    burst = burst || integrationBurst
    integrationSections.push({
      title: `${integrationBurst ? '⚠️ ' : ''}Integration ${integrationId}: ${rows.length} of ${active + rows.length} mappings deactivated`,
      text: formatDeactivationLines(rows).join('\n'),
    })
  }

  return {
    persona: burst ? SlackPersona.WARNING_PROPAGATOR : SlackPersona.INFO_NOTIFIER,
    title: burst
      ? 'Octolens keyword mappings deactivated in bulk'
      : 'Octolens keyword mappings deactivated',
    sections: [
      {
        title: '',
        text: [
          `Window: ${formatUtc(since)} → ${formatUtc(until)}`,
          'Discovery no longer saw these keywords mapped to a project, so their mentions stop ' +
            'reaching it. If a keyword gets mapped again, the mention poll re-fetches its whole ' +
            'history automatically.',
        ].join('\n'),
      },
      ...integrationSections,
    ],
  }
}

async function reportDeactivations(
  deps: IOctolensMappingHealthDeps,
  states: IOctolensMappingDiscoveryState[],
  result: IOctolensMappingHealthResult,
): Promise<void> {
  const { since, until, fromCursor } = deactivationWindow(
    states[0].checkedAt,
    await deps.getKey(DEACTIVATIONS_CURSOR_KEY),
  )
  if (since >= until) {
    return
  }
  // Pin the initial lookback so a retry after a failed send does not slide the window forward.
  if (!fromCursor) {
    await deps.setKey(DEACTIVATIONS_CURSOR_KEY, since.toISOString())
  }

  const deactivations = await deps.listDeactivatedBetween(since, until)
  if (deactivations.length > 0) {
    const sent = await deps.sendAlert(buildDeactivationAlert(deactivations, states, since, until))
    if (!sent) {
      result.sendFailures++
      return
    }
    result.deactivationsReported = deactivations.length
  }

  await deps.setKey(DEACTIVATIONS_CURSOR_KEY, until.toISOString())
}

async function reportStaleIntegrations(
  deps: IOctolensMappingHealthDeps,
  states: IOctolensMappingDiscoveryState[],
  result: IOctolensMappingHealthResult,
): Promise<void> {
  const toAlert: IOctolensMappingDiscoveryState[] = []
  for (const state of states.filter(isStale)) {
    const alertedFor = await deps.getKey(staleAlertedKey(state.integrationId))
    if (alertedFor !== staleEpisodeId(state)) {
      toAlert.push(state)
    }
  }
  if (toAlert.length === 0) {
    return
  }

  if (!(await deps.sendAlert(buildStaleAlert(toAlert)))) {
    result.sendFailures++
    return
  }
  for (const state of toAlert) {
    await deps.setKey(
      staleAlertedKey(state.integrationId),
      staleEpisodeId(state),
      STALE_REMINDER_HOURS * 60 * 60,
    )
  }
  result.staleAlerted = toAlert.length
}

export async function runOctolensMappingHealthCheck(
  deps: IOctolensMappingHealthDeps,
): Promise<IOctolensMappingHealthResult> {
  const states = await deps.listStates()
  const result: IOctolensMappingHealthResult = {
    integrations: states.length,
    staleAlerted: 0,
    deactivationsReported: 0,
    sendFailures: 0,
  }
  if (states.length === 0) {
    return result
  }

  await reportDeactivations(deps, states, result)
  await reportStaleIntegrations(deps, states, result)
  return result
}
