import { z } from 'zod'

import type {
  SessionizeHoldReason,
  SessionizeMatchCandidate,
} from '@crowd/connectors/src/connectors/sessionize/matching'
import { suggestSessionizeMapping } from '@crowd/connectors/src/connectors/sessionize/suggestions'
import type {
  SessionizeSuggestion,
  SessionizeSuggestionKind,
  SessionizeUnresolvedReason,
} from '@crowd/connectors/src/connectors/sessionize/suggestions'
import type { ISessionizeEventForReview } from '@crowd/data-access-layer/src/sessionize'

export type EventMappingState = 'suggested' | 'unresolved' | 'confirmed' | 'ignored'
export type EventMappingMatch =
  | SessionizeSuggestionKind
  | SessionizeUnresolvedReason
  | 'project_not_in_cdp'

export interface EventMappingProject {
  segmentId: string
  name: string
  groupName: string
}

export interface EventMappingRow {
  platform: 'sessionize'
  eventKey: string
  eventName: string
  eventStart: string
  eventEnd: string
  state: EventMappingState
  match: EventMappingMatch
  cventEvent: { name: string; startDate: string; dayGap: number } | null
  suggestedProject: EventMappingProject | null
  hintProject: EventMappingProject | null
  project: EventMappingProject | null
  decidedAt: string | null
  canUndo: boolean
}

export interface SubprojectForMapping {
  id: string
  name: string
  slug: string
  sourceId: string | null
  groupName: string
}

interface SubprojectIndex {
  bySourceId: Map<string, SubprojectForMapping[]>
  bySlug: Map<string, SubprojectForMapping[]>
}

interface ResolvedSuggestion {
  suggestion: SessionizeSuggestion
  match: EventMappingMatch
  suggested: SubprojectForMapping | null
}

const candidateSchema = z.object({
  eventId: z.string(),
  eventName: z.string(),
  startDate: z.string(),
  projectId: z.string().nullable(),
  source: z.string().nullable(),
  dayGap: z.number(),
})

const evidenceSchema = z.object({
  candidates: z.array(candidateSchema),
  nearby: z.array(candidateSchema).optional(),
})

const holdReasonSchema = z.enum([
  'no_match',
  'ambiguous',
  'not_cvent',
  'no_segment',
]) satisfies z.ZodType<SessionizeHoldReason>

function parseStored<T extends z.ZodTypeAny>(
  schema: T,
  value: unknown,
  eventKey: string,
  field: string,
): z.infer<T> {
  const parsed = schema.safeParse(value)
  if (!parsed.success) {
    throw new Error(
      `Sessionize event ${eventKey} has a malformed ${field}: ${parsed.error.message}`,
    )
  }
  return parsed.data
}

function groupBy(
  subprojects: SubprojectForMapping[],
  keyOf: (subproject: SubprojectForMapping) => string | null,
): Map<string, SubprojectForMapping[]> {
  const groups = new Map<string, SubprojectForMapping[]>()
  for (const subproject of subprojects) {
    const key = keyOf(subproject)
    if (key !== null) {
      const group = groups.get(key)
      if (group) {
        group.push(subproject)
      } else {
        groups.set(key, [subproject])
      }
    }
  }
  return groups
}

function indexSubprojects(subprojects: SubprojectForMapping[]): SubprojectIndex {
  return {
    bySourceId: groupBy(subprojects, ({ sourceId }) => sourceId && sourceId.toLowerCase()),
    bySlug: groupBy(subprojects, ({ slug }) => slug),
  }
}

function single(
  groups: Map<string, SubprojectForMapping[]>,
  key: string | null,
): SubprojectForMapping | null {
  const group = key === null ? undefined : groups.get(key)
  return group?.length === 1 ? group[0] : null
}

function subprojectOfEvent(
  index: SubprojectIndex,
  event: SessionizeMatchCandidate | null,
): SubprojectForMapping | null {
  if (event === null || event.projectId === null) {
    return null
  }
  return single(index.bySourceId, event.projectId.toLowerCase())
}

function toCandidate(stored: z.infer<typeof candidateSchema>): SessionizeMatchCandidate {
  return {
    eventId: stored.eventId,
    eventName: stored.eventName,
    startDate: stored.startDate,
    projectId: stored.projectId,
    source: stored.source,
    dayGap: stored.dayGap,
  }
}

function resolveSuggestion(
  event: ISessionizeEventForReview,
  index: SubprojectIndex,
): ResolvedSuggestion {
  const evidence = parseStored(evidenceSchema, event.evidence, event.eventKey, 'evidence')
  const suggestion = suggestSessionizeMapping({
    eventName: event.eventName,
    holdReason: parseStored(holdReasonSchema, event.holdReason, event.eventKey, 'hold reason'),
    candidates: evidence.candidates.map(toCandidate),
    nearby: evidence.nearby?.map(toCandidate),
  })

  if (suggestion.kind !== null) {
    const suggested =
      suggestion.kind === 'project_alias'
        ? single(index.bySlug, suggestion.projectSlug)
        : subprojectOfEvent(index, suggestion.cventEvent)
    return {
      suggestion,
      match: suggested === null ? 'project_not_in_cdp' : suggestion.kind,
      suggested,
    }
  }
  if (suggestion.reason !== null) {
    return { suggestion, match: suggestion.reason, suggested: null }
  }
  throw new Error(`Sessionize event ${event.eventKey} has neither a suggestion nor a reason!`)
}

function toProject(subproject: SubprojectForMapping | null): EventMappingProject | null {
  return subproject === null
    ? null
    : { segmentId: subproject.id, name: subproject.name, groupName: subproject.groupName }
}

function confirmedProject(event: ISessionizeEventForReview): EventMappingProject {
  const { segmentId, segmentName, projectGroupName } = event
  if (segmentId === null || segmentName === null || projectGroupName === null) {
    throw new Error(`Confirmed Sessionize event ${event.eventKey} has no subproject to show!`)
  }
  return { segmentId, name: segmentName, groupName: projectGroupName }
}

function stateOf(
  event: ISessionizeEventForReview,
  suggested: SubprojectForMapping | null,
): EventMappingState {
  if (event.status === 'matched') {
    return 'confirmed'
  }
  if (event.status === 'excluded') {
    return 'ignored'
  }
  return suggested === null ? 'unresolved' : 'suggested'
}

function buildRow(event: ISessionizeEventForReview, index: SubprojectIndex): EventMappingRow {
  const { suggestion, match, suggested } = resolveSuggestion(event, index)
  const state = stateOf(event, suggested)
  const { cventEvent } = suggestion
  return {
    platform: 'sessionize',
    eventKey: event.eventKey,
    eventName: event.eventName,
    eventStart: event.eventStart,
    eventEnd: event.eventEnd,
    state,
    match,
    cventEvent:
      cventEvent === null
        ? null
        : {
            name: cventEvent.eventName,
            startDate: cventEvent.startDate,
            dayGap: cventEvent.dayGap,
          },
    suggestedProject: toProject(suggested),
    hintProject: toProject(subprojectOfEvent(index, suggestion.hintCventEvent)),
    project: state === 'confirmed' ? confirmedProject(event) : null,
    decidedAt: event.reviewedAt,
    canUndo: event.reviewedAt !== null && event.lockedAt === null,
  }
}

export function buildEventMappingRows(
  events: ISessionizeEventForReview[],
  subprojects: SubprojectForMapping[],
): { rows: EventMappingRow[]; counts: Record<EventMappingState, number> } {
  const index = indexSubprojects(subprojects)
  const rows = events.map((event) => buildRow(event, index))
  const counts: Record<EventMappingState, number> = {
    suggested: 0,
    unresolved: 0,
    confirmed: 0,
    ignored: 0,
  }
  for (const row of rows) {
    counts[row.state] += 1
  }
  return { rows, counts }
}

export function suggestedCventEventId(
  event: ISessionizeEventForReview,
  subprojects: SubprojectForMapping[],
  segmentId: string,
): string | null {
  const { suggestion, suggested } = resolveSuggestion(event, indexSubprojects(subprojects))
  if (suggested === null || suggested.id !== segmentId || suggestion.cventEvent === null) {
    return null
  }
  return suggestion.cventEvent.eventId
}
