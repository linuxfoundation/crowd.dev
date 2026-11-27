import { CVENT_SOURCE, compareEventIds, normalizeEventName } from './matching'
import type { SessionizeHoldReason, SessionizeMatchCandidate } from './matching'
import { isNameContained, nameSimilarity } from './nameSimilarity'

const SIMILAR_NAME_THRESHOLD = 0.85
const SIMILAR_NAME_MARGIN = 0.1
const HINT_THRESHOLD = 0.7

export type SessionizeSuggestionKind =
  | 'same_name'
  | 'similar_name'
  | 'name_contained'
  | 'sub_event'
  | 'project_alias'
export type SessionizeUnresolvedReason =
  | 'ambiguous'
  | 'no_cvent_event'
  | 'main_event_not_in_cdp'
  | 'not_checked'

export interface SessionizeSuggestion {
  kind: SessionizeSuggestionKind | null
  reason: SessionizeUnresolvedReason | null
  cventEvent: SessionizeMatchCandidate | null
  projectSlug: string | null
  hintCventEvent: SessionizeMatchCandidate | null
}

export interface SessionizeSuggestionInput {
  eventName: string
  holdReason: SessionizeHoldReason
  candidates: SessionizeMatchCandidate[]
  nearby?: SessionizeMatchCandidate[]
}

interface ScoredEvent {
  event: SessionizeMatchCandidate
  score: number
}

const SUB_EVENT_NAME =
  /maintainer|lightning talk|contribfest|co-located|poster session|\b(?:events|workshop) at\b/i

const SUB_EVENT_PATTERNS: {
  pattern: RegExp
  mainEventName: (match: RegExpMatchArray) => string
}[] = [
  {
    pattern:
      /^(?:maintainer summit|maintainer track[^:]*|project lightning talk[^:]*)\s*:\s*(.+)$/i,
    mainEventName: (match) => match[1],
  },
  {
    pattern: /^cncf[- ]hosted co-located events (.+)$/i,
    mainEventName: (match) => `KubeCon CloudNativeCon ${match[1]}`,
  },
  {
    pattern: /^(.+?)\s+-\s+(?:maintainer track.*|poster sessions)$/i,
    mainEventName: (match) => match[1],
  },
  {
    pattern: /^sonic (?:events|workshop) at (.+)$/i,
    mainEventName: (match) => match[1],
  },
]

const PROJECT_ALIASES: { pattern: RegExp; projectSlug: string }[] = [
  { pattern: /pytorch/i, projectSlug: 'ptproject' },
  { pattern: /sonic/i, projectSlug: 'sonic-foundation' },
  { pattern: /jupyter/i, projectSlug: 'project-jupyter' },
  { pattern: /\buser!/i, projectSlug: 'rcons' },
  { pattern: /\bxen\b/i, projectSlug: 'xen' },
  { pattern: /opensearchcon/i, projectSlug: 'opensearch-foundation' },
  { pattern: /agntcon/i, projectSlug: 'agentic-ai-foundation' },
  { pattern: /bazelcon/i, projectSlug: 'community-for-bazel' },
  { pattern: /\bp4\b|europ4/i, projectSlug: 'p4-project' },
]

function suggestionOf(overrides: Partial<SessionizeSuggestion>): SessionizeSuggestion {
  return {
    kind: null,
    reason: null,
    cventEvent: null,
    projectSlug: null,
    hintCventEvent: null,
    ...overrides,
  }
}

function scoreEvents(eventName: string, events: SessionizeMatchCandidate[]): ScoredEvent[] {
  return events
    .map((event) => ({ event, score: nameSimilarity(eventName, event.eventName) }))
    .sort((a, b) => b.score - a.score || compareEventIds(a.event, b.event))
}

function isClearlyTop(scored: ScoredEvent[]): boolean {
  if (scored.length === 0 || scored[0].score < SIMILAR_NAME_THRESHOLD) {
    return false
  }
  return scored.length === 1 || scored[0].score - scored[1].score >= SIMILAR_NAME_MARGIN
}

function relabelSubEvent(
  eventName: string,
  kind: 'similar_name' | 'name_contained',
  event: SessionizeMatchCandidate,
): SessionizeSuggestion {
  return suggestionOf({
    kind: SUB_EVENT_NAME.test(eventName) ? 'sub_event' : kind,
    cventEvent: event,
  })
}

function sameNameEvent(
  holdReason: SessionizeHoldReason,
  candidates: SessionizeMatchCandidate[],
): SessionizeMatchCandidate | null {
  if (holdReason === 'not_cvent') {
    return candidates.length === 1 ? candidates[0] : null
  }
  if (holdReason === 'no_segment') {
    const cventCandidates = candidates.filter(({ source }) => source === CVENT_SOURCE)
    return cventCandidates.length === 1 ? cventCandidates[0] : null
  }
  return null
}

function preferredCopy(
  kept: SessionizeMatchCandidate,
  other: SessionizeMatchCandidate,
): SessionizeMatchCandidate {
  const keptIsCvent = kept.source === CVENT_SOURCE
  if (keptIsCvent !== (other.source === CVENT_SOURCE)) {
    return keptIsCvent ? kept : other
  }
  const order = kept.dayGap - other.dayGap || compareEventIds(kept, other)
  return order <= 0 ? kept : other
}

function onePerName(events: SessionizeMatchCandidate[]): SessionizeMatchCandidate[] {
  const byName = new Map<string, SessionizeMatchCandidate>()
  for (const event of events) {
    const name = normalizeEventName(event.eventName)
    const kept = byName.get(name)
    byName.set(name, kept === undefined ? event : preferredCopy(kept, event))
  }
  return [...byName.values()]
}

function findMainEventName(eventName: string): string | null {
  for (const { pattern, mainEventName } of SUB_EVENT_PATTERNS) {
    const match = eventName.match(pattern)
    if (match) {
      return mainEventName(match)
    }
  }
  return null
}

export function suggestSessionizeMapping(input: SessionizeSuggestionInput): SessionizeSuggestion {
  const { eventName, holdReason, candidates } = input

  const sameName = sameNameEvent(holdReason, candidates)
  if (sameName !== null) {
    return suggestionOf({ kind: 'same_name', cventEvent: sameName })
  }

  if (input.nearby === undefined) {
    return suggestionOf({ reason: 'not_checked' })
  }
  const nearby = onePerName(input.nearby)

  const scored = scoreEvents(eventName, nearby)
  if (isClearlyTop(scored)) {
    return relabelSubEvent(eventName, 'similar_name', scored[0].event)
  }

  const contained = nearby.filter(
    (event) =>
      isNameContained(eventName, event.eventName) || isNameContained(event.eventName, eventName),
  )
  if (contained.length === 1) {
    return relabelSubEvent(eventName, 'name_contained', contained[0])
  }

  const mainEventName = findMainEventName(eventName)
  if (mainEventName !== null) {
    const mainEvent = scoreEvents(mainEventName, nearby)[0]
    if (mainEvent && mainEvent.score >= SIMILAR_NAME_THRESHOLD) {
      return suggestionOf({ kind: 'sub_event', cventEvent: mainEvent.event })
    }
  }

  const alias = PROJECT_ALIASES.find(({ pattern }) => pattern.test(eventName))
  if (alias) {
    return suggestionOf({ kind: 'project_alias', projectSlug: alias.projectSlug })
  }

  if (mainEventName !== null) {
    return suggestionOf({ reason: 'main_event_not_in_cdp' })
  }

  const hint = scored.length > 0 && scored[0].score >= HINT_THRESHOLD ? scored[0].event : null
  if (holdReason === 'ambiguous' || hint !== null) {
    return suggestionOf({ reason: 'ambiguous', hintCventEvent: hint })
  }
  return suggestionOf({ reason: 'no_cvent_event' })
}
