import type { SessionizeConsoleEvent } from './consoleEvents'

export const MATCHING_RULE_VERSION = 4
export const MATCH_WINDOW_DAYS = 21
export const SUGGESTION_WINDOW_DAYS = 3

const MILLISECONDS_PER_DAY = 86_400_000
const CVENT_SOURCE = 'cvent'

const NAME_SYNONYMS = new Map<string, string>([
  ['na', 'north america'],
  ['eu', 'europe'],
  ['agl', 'automotive grade linux'],
  ['amm', 'all member meeting'],
  ['conf', 'conference'],
  ['virtual', ''],
])

export interface CventCatalogueEvent {
  eventId: string
  eventName: string
  startDate: string
  projectId: string | null
  projectSlug: string | null
  source: string | null
}

export type SubprojectLookup = (
  projectSlug: string | null,
  projectId: string | null,
) => string | null

export type SessionizeHoldReason = 'no_match' | 'ambiguous' | 'not_cvent' | 'no_segment'

export interface SessionizeMatchCandidate {
  eventId: string
  eventName: string
  startDate: string
  projectId: string | null
  source: string | null
  dayGap: number
}

export interface SessionizeEventDecision {
  event: SessionizeConsoleEvent
  status: 'matched' | 'held'
  holdReason: SessionizeHoldReason | null
  cventEventId: string | null
  catalogueSource: string | null
  segmentId: string | null
  rule: 'exact_name' | null
  ruleVersion: number
  evidence: {
    candidates: SessionizeMatchCandidate[]
    nearby: SessionizeMatchCandidate[]
  }
}

interface FoundCandidate {
  catalogueEvent: CventCatalogueEvent
  candidate: SessionizeMatchCandidate
}

interface CatalogueScan {
  found: FoundCandidate[]
  nearby: SessionizeMatchCandidate[]
}

export function normalizeEventName(name: string): string {
  return name
    .normalize('NFKC')
    .toLowerCase()
    .replaceAll('&', ' and ')
    .split(/[^\p{L}\p{M}\p{N}]+/u)
    .map((token) => NAME_SYNONYMS.get(token) ?? token)
    .filter(Boolean)
    .join(' ')
}

function dayGap(first: string, second: string): number {
  return Math.round(
    Math.abs(Date.parse(`${first.slice(0, 10)}T00:00:00Z`) - Date.parse(`${second}T00:00:00Z`)) /
      MILLISECONDS_PER_DAY,
  )
}

function toCandidate(entry: CventCatalogueEvent, gap: number): SessionizeMatchCandidate {
  return {
    eventId: entry.eventId,
    eventName: entry.eventName,
    startDate: entry.startDate,
    projectId: entry.projectId,
    source: entry.source,
    dayGap: gap,
  }
}

function compareEventIds(
  first: SessionizeMatchCandidate,
  second: SessionizeMatchCandidate,
): number {
  if (first.eventId === second.eventId) {
    return 0
  }
  return first.eventId < second.eventId ? -1 : 1
}

function scanCatalogue(
  event: SessionizeConsoleEvent,
  catalogue: CventCatalogueEvent[],
): CatalogueScan {
  const normalizedName = normalizeEventName(event.eventName)
  const found: FoundCandidate[] = []
  const nearby: SessionizeMatchCandidate[] = []
  for (const entry of catalogue) {
    const gap = dayGap(entry.startDate, event.eventStart)
    if (gap <= MATCH_WINDOW_DAYS) {
      const candidate = toCandidate(entry, gap)
      if (gap <= SUGGESTION_WINDOW_DAYS) {
        nearby.push(candidate)
      }
      if (normalizedName !== '' && normalizeEventName(entry.eventName) === normalizedName) {
        found.push({ catalogueEvent: entry, candidate })
      }
    }
  }
  return { found, nearby: nearby.sort(compareEventIds) }
}

export function matchSessionizeEvent(
  event: SessionizeConsoleEvent,
  catalogue: CventCatalogueEvent[],
  subprojectFor: SubprojectLookup,
): SessionizeEventDecision {
  const { found, nearby } = scanCatalogue(event, catalogue)
  const candidates = found.map(({ candidate }) => candidate)
  const held = (
    holdReason: SessionizeHoldReason,
    candidate: CventCatalogueEvent | null = null,
  ): SessionizeEventDecision => ({
    event,
    status: 'held',
    holdReason,
    cventEventId: candidate?.eventId ?? null,
    catalogueSource: candidate?.source ?? null,
    segmentId: null,
    rule: null,
    ruleVersion: MATCHING_RULE_VERSION,
    evidence: { candidates, nearby },
  })

  if (found.length === 0) {
    return held('no_match')
  }

  const cventFound = found.filter(({ catalogueEvent }) => catalogueEvent.source === CVENT_SOURCE)
  if (cventFound.length > 1) {
    return held('ambiguous')
  }
  if (cventFound.length === 0) {
    return found.length === 1 ? held('not_cvent', found[0].catalogueEvent) : held('ambiguous')
  }

  const { catalogueEvent } = cventFound[0]
  const segmentId = subprojectFor(catalogueEvent.projectSlug, catalogueEvent.projectId)
  if (segmentId === null) {
    return held('no_segment', catalogueEvent)
  }
  return {
    event,
    status: 'matched',
    holdReason: null,
    cventEventId: catalogueEvent.eventId,
    catalogueSource: catalogueEvent.source,
    segmentId,
    rule: 'exact_name',
    ruleVersion: MATCHING_RULE_VERSION,
    evidence: { candidates, nearby: [] },
  }
}
