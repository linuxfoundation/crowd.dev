import type { IOctolensMentionPollUnit } from '@crowd/data-access-layer/src/octolens'

const MAX_POSTGRES_INT = 2_147_483_647
const MAX_REWIND_YEARS = 1
const ISO_DATE =
  /^(\d{4})-(\d{2})-(\d{2})(?:T(?:[01]\d|2[0-3]):[0-5]\d(?::[0-5]\d(?:\.\d{1,3})?)?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d))?$/

export type OctolensBackfillArgs =
  | { mode: 'keywords'; keywordIds: number[]; dryRun: boolean }
  | { mode: 'from'; startDate: string; dryRun: boolean }

export type OctolensBackfillPlan =
  | {
      mode: 'keywords'
      unit: IOctolensMentionPollUnit
      keywordIdsToQueue: number[]
      alreadyQueued: number[]
    }
  | {
      mode: 'from'
      unit: IOctolensMentionPollUnit
      startDate: string
      previousStartDate: string | null
    }

export interface IOctolensBackfillState {
  units: IOctolensMentionPollUnit[]
  activeMappingCounts: Map<number, number>
}

export const OCTOLENS_BACKFILL_USAGE = [
  'Usage: backfill-octolens-mentions (--keywords <id,id,...> | --from <ISO date>) [--dry-run]',
  '  --from takes YYYY-MM-DD (UTC midnight) or an ISO timestamp with a zone, at most 1 year back.',
  '  Exit codes: 0 done or nothing to do, 1 invalid input or state, 2 a poll run is in progress (retry).',
].join('\n')

export function parseOctolensBackfillArgs(argv: string[], now: Date): OctolensBackfillArgs {
  const values = new Map<string, string>()
  let dryRun = false

  const args = argv.filter((arg) => arg !== '--')
  for (let i = 0; i < args.length; i++) {
    const arg = args[i]
    if (arg === '--dry-run') {
      if (dryRun) throw new Error('--dry-run given more than once')
      dryRun = true
      continue
    }
    if (arg !== '--keywords' && arg !== '--from') {
      throw new Error(`unknown argument "${arg}"`)
    }
    if (values.has(arg)) throw new Error(`${arg} given more than once`)
    const value = args[i + 1]
    if (value === undefined || value.startsWith('--')) throw new Error(`${arg} needs a value`)
    values.set(arg, value)
    i++
  }

  const keywords = values.get('--keywords')
  const from = values.get('--from')
  if ((keywords === undefined) === (from === undefined)) {
    throw new Error('pass exactly one of --keywords or --from')
  }

  if (keywords !== undefined) {
    return { mode: 'keywords', keywordIds: parseKeywordIds(keywords), dryRun }
  }
  return { mode: 'from', startDate: parseStartDate(from as string, now), dryRun }
}

// Repeats collapse to one entry so the plan and its log list each keyword once.
function parseKeywordIds(raw: string): number[] {
  const ids = raw.split(',').map((part) => {
    const trimmed = part.trim()
    const id = Number(trimmed)
    if (!/^\d+$/.test(trimmed) || id < 1 || id > MAX_POSTGRES_INT) {
      throw new Error(`invalid keyword id "${part}": expected a positive integer`)
    }
    return id
  })
  return [...new Set(ids)]
}

function parseStartDate(raw: string, now: Date): string {
  const match = ISO_DATE.exec(raw)
  if (!match) {
    throw new Error(`invalid --from "${raw}": expected YYYY-MM-DD or an ISO timestamp with a zone`)
  }
  const [year, month, day] = match.slice(1, 4).map(Number)
  const calendarDay = new Date(Date.UTC(year, month - 1, day))
  const date = new Date(raw)
  if (
    calendarDay.getUTCFullYear() !== year ||
    calendarDay.getUTCMonth() !== month - 1 ||
    calendarDay.getUTCDate() !== day ||
    Number.isNaN(date.getTime())
  ) {
    throw new Error(`invalid --from "${raw}": not a real date`)
  }
  if (date.getTime() > now.getTime()) {
    throw new Error(`invalid --from "${raw}": in the future`)
  }
  const earliest = new Date(now)
  earliest.setUTCFullYear(earliest.getUTCFullYear() - MAX_REWIND_YEARS)
  if (date.getTime() < earliest.getTime()) {
    throw new Error(
      `invalid --from "${raw}": more than ${MAX_REWIND_YEARS} year back (earliest ${earliest.toISOString()})`,
    )
  }
  return date.toISOString()
}

export function planOctolensBackfill(
  args: OctolensBackfillArgs,
  state: IOctolensBackfillState,
): OctolensBackfillPlan {
  if (state.units.length !== 1) {
    throw new Error(
      `expected exactly one octolens mentionPoll sync unit, found ${state.units.length}`,
    )
  }
  const unit = state.units[0]
  if (unit.status !== 'active') {
    throw new Error(
      `octolens mentionPoll sync unit ${unit.id} is ${unit.status}, not active: fix the unit first`,
    )
  }

  if (args.mode === 'keywords') {
    const unmapped = args.keywordIds.filter((id) => !state.activeMappingCounts.has(id))
    if (unmapped.length > 0) {
      throw new Error(`keywords without an active subproject mapping: ${unmapped.join(', ')}`)
    }
    const ambiguous = args.keywordIds.find((id) => (state.activeMappingCounts.get(id) ?? 0) > 1)
    if (ambiguous !== undefined) {
      throw new Error(`keyword ${ambiguous} is mapped to more than one subproject`)
    }
    const queued = new Set(queuedKeywordIds(unit.watermark))
    return {
      mode: 'keywords',
      unit,
      keywordIdsToQueue: args.keywordIds.filter((id) => !queued.has(id)),
      alreadyQueued: args.keywordIds.filter((id) => queued.has(id)),
    }
  }

  const previousStartDate =
    typeof unit.watermark?.startDate === 'string' ? unit.watermark.startDate : null
  if (previousStartDate !== null) {
    const previous = new Date(previousStartDate).getTime()
    if (!Number.isNaN(previous) && new Date(args.startDate).getTime() >= previous) {
      throw new Error(
        `--from ${args.startDate} is not earlier than the current start date ${previousStartDate}`,
      )
    }
  }
  return { mode: 'from', unit, startDate: args.startDate, previousStartDate }
}

function queuedKeywordIds(watermark: Record<string, unknown> | null): number[] {
  const backfill = watermark?.backfill
  if (!Array.isArray(backfill)) return []
  // Matches the poll, which drops entries whose cursor is missing or not a string.
  return backfill
    .filter((item) => {
      const cursor = (item as { cursor?: unknown } | null)?.cursor
      return cursor === null || typeof cursor === 'string'
    })
    .map((item) => (item as { keywordId?: unknown }).keywordId)
    .filter((id): id is number => Number.isInteger(id))
}
