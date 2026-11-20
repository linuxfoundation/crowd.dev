import type {
  CventCatalogueEvent,
  SessionizeEventDecision,
  SessionizeHoldReason,
  SubprojectLookup,
} from '@crowd/connectors/src/connectors/sessionize/matching'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import {
  ISessionizeEventDecisionInput,
  IStoredSessionizeEventMapping,
  SessionizeDecisionOutcome,
  deactivateUnlistedSessionizeEvents,
  listSessionizeEventMappings,
  upsertSessionizeEventDecision,
} from '@crowd/data-access-layer/src/sessionize'
import type { Logger } from '@crowd/logging'

import type { CatalogueSource } from './credlyTemplateResolve'

const CVENT_CATALOGUE_QUERY = `
  SELECT "eventId", "eventName", "startDate", "projectId"
  FROM (
    SELECT EVENT_ID AS "eventId", EVENT_NAME AS "eventName",
           TO_VARCHAR(EVENT_START_DATE::date, 'YYYY-MM-DD') AS "startDate",
           PROJECT_ID AS "projectId"
    FROM analytics.silver_dim.events
    WHERE EVENT_ID IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_ID ORDER BY UPDATED_AT DESC NULLS LAST) = 1
  )
  WHERE "eventName" IS NOT NULL AND "startDate" IS NOT NULL
  ORDER BY "eventId"
`

interface CventCatalogueRow {
  eventId: string
  eventName: string
  startDate: string
  projectId: string | null
}

function isCalendarDate(value: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false
  const parsed = new Date(`${value}T00:00:00Z`)
  return !Number.isNaN(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value
}

export async function loadCventCatalogue(source: CatalogueSource): Promise<CventCatalogueEvent[]> {
  const rows = await source.run<CventCatalogueRow>(CVENT_CATALOGUE_QUERY)
  if (rows.length === 0) {
    throw new Error('The Cvent event catalogue is empty, refusing to match against it!')
  }
  const invalid = rows.filter((row) => !isCalendarDate(String(row.startDate)))
  if (invalid.length > 0) {
    const examples = invalid.slice(0, 5).map((row) => `${row.eventId}=${row.startDate}`)
    throw new Error(
      `${invalid.length} Cvent catalogue events have an invalid start date: ${examples.join(', ')}`,
    )
  }
  return rows.map((row) => ({
    eventId: row.eventId,
    eventName: row.eventName,
    startDate: row.startDate,
    projectId: row.projectId ?? null,
    projectSlug: null,
  }))
}

export function buildSubprojectLookup(
  rows: { id: string; slug: string; sourceId: string }[],
): SubprojectLookup {
  const idsBySourceId = new Map<string, string[]>()
  for (const row of rows) {
    const key = row.sourceId.toLowerCase()
    idsBySourceId.set(key, [...(idsBySourceId.get(key) ?? []), row.id])
  }
  return (_projectSlug, projectId) => {
    if (!projectId) return null
    const ids = idsBySourceId.get(projectId.toLowerCase())
    return ids?.length === 1 ? ids[0] : null
  }
}

export interface SessionizeMatchingSummary {
  listed: number
  matched: number
  held: Record<SessionizeHoldReason, number>
  outcomes: Record<SessionizeDecisionOutcome, number>
  deactivated: number
}

function toDecisionInput(decision: SessionizeEventDecision): ISessionizeEventDecisionInput {
  return {
    ...decision.event,
    status: decision.status,
    holdReason: decision.holdReason,
    cventEventId: decision.cventEventId,
    segmentId: decision.segmentId,
    rule: decision.rule,
    ruleVersion: decision.ruleVersion,
    evidence: decision.evidence,
  }
}

function reviewTarget(
  row: Pick<ISessionizeEventDecisionInput, 'status' | 'holdReason' | 'segmentId' | 'cventEventId'>,
) {
  return {
    status: row.status,
    holdReason: row.holdReason,
    segmentId: row.segmentId,
    cventEventId: row.cventEventId,
  }
}

function emptySummary(): SessionizeMatchingSummary {
  return {
    listed: 0,
    matched: 0,
    held: { no_match: 0, ambiguous: 0, no_segment: 0 },
    outcomes: { inserted: 0, updated: 0, unchanged: 0, protected: 0 },
    deactivated: 0,
  }
}

export async function applySessionizeDecisions(
  qx: QueryExecutor,
  decisions: SessionizeEventDecision[],
  log: Logger,
): Promise<SessionizeMatchingSummary> {
  return qx.tx(async (tx) => {
    const summary = emptySummary()
    const stored = new Map<string, IStoredSessionizeEventMapping>(
      (await listSessionizeEventMappings(tx)).map((row) => [row.eventKey, row]),
    )
    for (const decision of decisions) {
      summary.listed += 1
      if (decision.status === 'matched') summary.matched += 1
      if (decision.holdReason) summary.held[decision.holdReason] += 1

      const input = toDecisionInput(decision)
      const outcome = await upsertSessionizeEventDecision(tx, input)
      summary.outcomes[outcome] += 1
      if (outcome === 'protected') {
        const row = stored.get(input.eventKey)
        log.warn(
          {
            event: 'sessionize_decision_needs_review',
            eventKey: input.eventKey,
            wanted: reviewTarget(input),
            stored: row ? reviewTarget(row) : null,
          },
          'sessionize event is reviewed or locked, the new decision needs a review',
        )
      }
    }
    summary.deactivated = await deactivateUnlistedSessionizeEvents(
      tx,
      decisions.map((decision) => decision.event.eventKey),
    )
    return summary
  })
}
