import type { SessionizeConsoleEvent } from './consoleEvents'

export const MATCHING_RULE_VERSION = 3
export const MATCH_WINDOW_DAYS = 21

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
  }
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

function findCandidates(
  event: SessionizeConsoleEvent,
  catalogue: CventCatalogueEvent[],
): { catalogueEvent: CventCatalogueEvent; candidate: SessionizeMatchCandidate }[] {
  const normalizedName = normalizeEventName(event.eventName)
  if (normalizedName === '') {
    return []
  }
  return catalogue
    .filter((entry) => normalizeEventName(entry.eventName) === normalizedName)
    .map((entry) => ({
      catalogueEvent: entry,
      candidate: {
        eventId: entry.eventId,
        eventName: entry.eventName,
        startDate: entry.startDate,
        projectId: entry.projectId,
        source: entry.source,
        dayGap: dayGap(entry.startDate, event.eventStart),
      },
    }))
    .filter(({ candidate }) => candidate.dayGap <= MATCH_WINDOW_DAYS)
}

export function matchSessionizeEvent(
  event: SessionizeConsoleEvent,
  catalogue: CventCatalogueEvent[],
  subprojectFor: SubprojectLookup,
): SessionizeEventDecision {
  const found = findCandidates(event, catalogue)
  const evidence = { candidates: found.map(({ candidate }) => candidate) }
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
    evidence,
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
    evidence,
  }
}
