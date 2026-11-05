import type {
  IDeactivatedOctolensKeywordMapping,
  IOctolensMappingDiscoveryHealth,
} from '@crowd/data-access-layer/src/octolens/health'
import { Logger } from '@crowd/logging'
import { SlackMessageSection, SlackPersona, escapeSlackText } from '@crowd/slack'

const STALE_AFTER_HOURS = 3
const STALE_REMINDER_HOURS = 24
const BURST_MIN_DEACTIVATIONS = 3
const BURST_MIN_SHARE = 0.25
const CURSOR_SAFETY_MARGIN_MINUTES = 5
const INITIAL_LOOKBACK_HOURS = 24
const MAX_LISTED_KEYWORDS = 25
// Under @crowd/slack's 2,900-char section block, so the list is never split or pruned.
export const MAX_SECTION_CHARS = 2800
const MAX_NAME_LENGTH = 100
const OVERFLOW_NOTE_RESERVE = 20

export const DEACTIVATIONS_CURSOR_KEY = 'octolens-mapping-health:deactivations-reported-until'
export const STALE_ALERTED_KEY = 'octolens-mapping-health:stale-alerted'

const BACKFILL_COMMAND =
  'pnpm --filter @crowd/connectors-worker run script:backfill-octolens-mentions --'

const HOUR_MS = 60 * 60 * 1000
const MINUTE_MS = 60 * 1000

export interface IOctolensMappingHealthAlert {
  persona: SlackPersona
  title: string
  sections: SlackMessageSection[]
}

export interface IOctolensMappingHealthDeps {
  log: Logger
  getHealth: () => Promise<IOctolensMappingDiscoveryHealth | null>
  listDeactivatedBetween: (
    since: Date,
    until: Date,
  ) => Promise<IDeactivatedOctolensKeywordMapping[]>
  getKey: (key: string) => Promise<string | null>
  setKey: (key: string, value: string, expireAfterSeconds?: number) => Promise<void>
  sendAlert: (alert: IOctolensMappingHealthAlert) => Promise<boolean>
}

export interface IOctolensMappingHealthResult {
  activeMappings: number
  staleAlerted: boolean
  deactivationsReported: number
  failures: number
}

function hoursSince(from: Date, now: Date): number {
  return (now.getTime() - from.getTime()) / HOUR_MS
}

function isStale(health: IOctolensMappingDiscoveryHealth): boolean {
  return hoursSince(health.lastDiscoveredAt, health.checkedAt) > STALE_AFTER_HOURS
}

export function isDeactivationBurst(deactivated: number, activeMappings: number): boolean {
  if (deactivated >= BURST_MIN_DEACTIVATIONS) {
    return true
  }
  const total = activeMappings + deactivated
  return total > 0 && deactivated / total >= BURST_MIN_SHARE
}

// One discovery run deactivates in one transaction, so its rows share one now() timestamp.
// A keyword moved to another project is a deliberate change, not discovery losing it.
export function hasDeactivationBurst(
  deactivations: IDeactivatedOctolensKeywordMapping[],
  activeMappings: number,
): boolean {
  const perRun = new Map<number, number>()
  for (const row of deactivations.filter((r) => r.nowMappedToSegmentName === null)) {
    const at = row.deactivatedAt.getTime()
    perRun.set(at, (perRun.get(at) ?? 0) + 1)
  }
  return [...perRun.values()].some((count) => isDeactivationBurst(count, activeMappings))
}

function deactivationWindow(
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

// Names come from Octolens and project data. Inside a code span Slack applies no `*`, `_` or
// `~` formatting; a backtick would end the span, so it becomes a look-alike.
export function formatName(name: string): string {
  const chars = Array.from(name.replace(/\s+/g, ' '))
  const short =
    chars.length > MAX_NAME_LENGTH
      ? `${chars.slice(0, MAX_NAME_LENGTH - 1).join('')}…`
      : chars.join('')
  return `\`${escapeSlackText(short.replace(/`/g, 'ˋ'))}\``
}

function buildStaleAlert(health: IOctolensMappingDiscoveryHealth): IOctolensMappingHealthAlert {
  return {
    persona: SlackPersona.WARNING_PROPAGATOR,
    title: 'Octolens mapping discovery looks stale',
    sections: [
      {
        title: '',
        text: [
          `Keyword-to-project mapping discovery has not written any mappings in over ${STALE_AFTER_HOURS} hours.`,
          `Last write: ${formatUtc(health.lastDiscoveredAt)} (${Math.floor(hoursSince(health.lastDiscoveredAt, health.checkedAt))}h ago)`,
          `Active mappings: ${health.activeMappings} across ${health.integrations} projects`,
          'Existing mappings keep routing mentions, but keywords added, moved or removed in ' +
            'Octolens are not picked up, and the mention poll skips mentions of new keywords.',
          'Check the `connectors-octolens-discovery` schedule and the connectors_worker logs: a ' +
            'failed run, an incomplete Octolens snapshot or a run that finds no mappings writes ' +
            'nothing, and so does a missing `CROWD_OCTOLENS_API_TOKEN`.',
          `Once discovery writes again, recover the skipped mentions with \`${BACKFILL_COMMAND} --from ${health.lastDiscoveredAt.toISOString()}\`, ` +
            'running it with `--dry-run` first. If it refuses because the date is not earlier ' +
            'than the current start date, the poll may already have read past some skipped mentions, ' +
            `so re-fetch the keywords mapped since then with \`${BACKFILL_COMMAND} --keywords KEYWORD_ID,KEYWORD_ID\`.`,
        ].join('\n'),
      },
    ],
  }
}

// Capped because @crowd/slack silently prunes oversized messages while still reporting
// success, which would move the cursor past deactivations nobody saw.
export function capLines(lines: string[], maxLines: number, maxChars: number): string {
  const budget = maxChars - OVERFLOW_NOTE_RESERVE
  const kept: string[] = []
  let length = 0
  for (const line of lines) {
    if (kept.length === maxLines || length + line.length + 1 > budget) {
      break
    }
    kept.push(line)
    length += line.length + 1
  }
  const omitted = lines.length - kept.length
  return [...kept, ...(omitted > 0 ? [`… and ${omitted} more`] : [])].join('\n')
}

function formatDeactivation(row: IDeactivatedOctolensKeywordMapping): string {
  return (
    `• ${formatName(row.keyword)} (${row.keywordId}) → ` +
    `${formatName(row.segmentName)} (${formatName(row.segmentSlug)}), ` +
    `deactivated ${formatUtc(row.deactivatedAt)}` +
    (row.integrationDeleted ? ', project has no Octolens keywords left' : '') +
    (row.nowMappedToSegmentName ? `, now mapped to ${formatName(row.nowMappedToSegmentName)}` : '')
  )
}

// Stable, so rows of one run keep the query's project and keyword order.
function newestFirst(
  deactivations: IDeactivatedOctolensKeywordMapping[],
): IDeactivatedOctolensKeywordMapping[] {
  return [...deactivations].sort((a, b) => b.deactivatedAt.getTime() - a.deactivatedAt.getTime())
}

function buildDeactivationAlert(
  deactivations: IDeactivatedOctolensKeywordMapping[],
  activeMappings: number,
  since: Date,
  until: Date,
): IOctolensMappingHealthAlert {
  const burst = hasDeactivationBurst(deactivations, activeMappings)

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
          'Discovery no longer saw these keywords mapped to the project, so their mentions stop ' +
            'reaching it. A keyword now mapped to another project goes there instead; any other ' +
            'keyword is skipped by the mention poll.',
          'Nothing is re-fetched automatically: once a skipped keyword is mapped again, re-fetch ' +
            `its history with \`${BACKFILL_COMMAND} --keywords KEYWORD_ID,KEYWORD_ID\`, running ` +
            'it with `--dry-run` first.',
        ].join('\n'),
      },
      {
        title: `Deactivated mappings (${deactivations.length}, ${activeMappings} still active)`,
        text: capLines(
          newestFirst(deactivations).map(formatDeactivation),
          MAX_LISTED_KEYWORDS,
          MAX_SECTION_CHARS,
        ),
      },
    ],
  }
}

async function reportDeactivations(
  deps: IOctolensMappingHealthDeps,
  health: IOctolensMappingDiscoveryHealth,
  result: IOctolensMappingHealthResult,
): Promise<void> {
  const { since, until, fromCursor } = deactivationWindow(
    health.checkedAt,
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
    const alert = buildDeactivationAlert(deactivations, health.activeMappings, since, until)
    if (!(await deps.sendAlert(alert))) {
      result.failures++
      deps.log.warn(
        { windowStart: since.toISOString(), deactivations: deactivations.length },
        'octolens deactivation alert failed to send, the next run retries from the same window start',
      )
      return
    }
    result.deactivationsReported = deactivations.length
  }

  await deps.setKey(DEACTIVATIONS_CURSOR_KEY, until.toISOString())
}

async function reportStaleness(
  deps: IOctolensMappingHealthDeps,
  health: IOctolensMappingDiscoveryHealth,
  result: IOctolensMappingHealthResult,
): Promise<void> {
  if (!isStale(health)) {
    return
  }
  const episode = health.lastDiscoveredAt.toISOString()
  if ((await deps.getKey(STALE_ALERTED_KEY)) === episode) {
    return
  }

  if (!(await deps.sendAlert(buildStaleAlert(health)))) {
    result.failures++
    deps.log.warn({ lastDiscoveredAt: episode }, 'octolens stale alert failed to send')
    return
  }
  await deps.setKey(STALE_ALERTED_KEY, episode, STALE_REMINDER_HOURS * 60 * 60)
  result.staleAlerted = true
}

async function runIsolated(
  deps: IOctolensMappingHealthDeps,
  result: IOctolensMappingHealthResult,
  name: string,
  check: () => Promise<void>,
): Promise<void> {
  try {
    await check()
  } catch (err) {
    result.failures++
    deps.log.error({ err }, `octolens mapping health: ${name} check failed`)
  }
}

// null means no active mapping, so discovery never completed a run (or the token was never set):
// a run that sees no mappings writes nothing, so it cannot empty a populated table.
export async function runOctolensMappingHealthCheck(
  deps: IOctolensMappingHealthDeps,
): Promise<IOctolensMappingHealthResult> {
  const health = await deps.getHealth()
  const result: IOctolensMappingHealthResult = {
    activeMappings: health?.activeMappings ?? 0,
    staleAlerted: false,
    deactivationsReported: 0,
    failures: 0,
  }
  if (!health) {
    return result
  }

  await runIsolated(deps, result, 'staleness', () => reportStaleness(deps, health, result))
  await runIsolated(deps, result, 'deactivations', () => reportDeactivations(deps, health, result))
  return result
}
