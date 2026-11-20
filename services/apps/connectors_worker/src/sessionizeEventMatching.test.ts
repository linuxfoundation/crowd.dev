import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { ConnectorHttp } from '@crowd/connectors'
import type { SessionizeEventDecision } from '@crowd/connectors/src/connectors/sessionize/matching'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { listActiveSubprojectSegmentSources } from '@crowd/data-access-layer/src/segments'
import {
  IStoredSessionizeEventMapping,
  SessionizeDecisionOutcome,
  deactivateUnlistedSessionizeEvents,
  listSessionizeEventMappings,
  upsertSessionizeEventDecision,
} from '@crowd/data-access-layer/src/sessionize'
import type { Logger } from '@crowd/logging'

import type { CatalogueSource } from './credlyTemplateResolve'
import {
  applySessionizeDecisions,
  buildSubprojectLookup,
  computeSessionizeDecisions,
  loadCventCatalogue,
} from './sessionizeEventMatching'

vi.mock('@crowd/data-access-layer/src/sessionize', () => ({
  upsertSessionizeEventDecision: vi.fn(),
  deactivateUnlistedSessionizeEvents: vi.fn(),
  listSessionizeEventMappings: vi.fn(),
}))

vi.mock('@crowd/data-access-layer/src/segments', () => ({
  listActiveSubprojectSegmentSources: vi.fn(),
}))

function fakeLog() {
  const warn = vi.fn()
  return { warn, log: { warn, info: vi.fn() } as unknown as Logger }
}

function stubSource(rows: Record<string, unknown>[]): CatalogueSource & { queries: string[] } {
  const queries: string[] = []
  return {
    queries,
    run: async <T>(query: string) => {
      queries.push(query)
      return rows as T[]
    },
  }
}

describe('loadCventCatalogue', () => {
  it('reads the latest row per event and maps it with no project slug', async () => {
    const source = stubSource([
      { eventId: 'ev-1', eventName: 'Fake Summit', startDate: '2027-04-28', projectId: 'p-1' },
      { eventId: 'ev-2', eventName: 'Fake Day', startDate: '2027-02-28', projectId: null },
    ])

    expect(await loadCventCatalogue(source, fakeLog().log)).toEqual([
      {
        eventId: 'ev-1',
        eventName: 'Fake Summit',
        startDate: '2027-04-28',
        projectId: 'p-1',
        projectSlug: null,
      },
      {
        eventId: 'ev-2',
        eventName: 'Fake Day',
        startDate: '2027-02-28',
        projectId: null,
        projectSlug: null,
      },
    ])
    expect(source.queries).toHaveLength(1)
    expect(source.queries[0]).toMatch(
      /QUALIFY ROW_NUMBER\(\) OVER \(PARTITION BY EVENT_ID ORDER BY UPDATED_AT DESC NULLS LAST\) = 1/,
    )
  })

  it('fails on an empty catalogue instead of holding every event', async () => {
    await expect(loadCventCatalogue(stubSource([]), fakeLog().log)).rejects.toThrow(/empty/)
  })

  it('logs and leaves out the rows without a name or start date', async () => {
    const source = stubSource([
      { eventId: 'ev-1', eventName: 'Fake Summit', startDate: '2027-04-28', projectId: null },
      { eventId: 'ev-2', eventName: null, startDate: '2027-04-28', projectId: null },
      { eventId: 'ev-3', eventName: 'Fake Day', startDate: null, projectId: null },
    ])
    const { warn, log } = fakeLog()

    expect((await loadCventCatalogue(source, log)).map((row) => row.eventId)).toEqual(['ev-1'])
    expect(warn).toHaveBeenCalledTimes(1)
    expect(warn.mock.calls[0][0]).toEqual({
      event: 'sessionize_catalogue_rows_skipped',
      count: 2,
      eventIds: ['ev-2', 'ev-3'],
    })
  })

  it.for(['2027-02-30', '2027-4-28', '28/04/2027'])(
    'fails on a start date that is not a calendar date (%s)',
    async (startDate) => {
      const source = stubSource([
        { eventId: 'ev-1', eventName: 'Fake Summit', startDate: '2027-04-28', projectId: null },
        { eventId: 'ev-2', eventName: 'Fake Day', startDate, projectId: null },
      ])
      await expect(loadCventCatalogue(source, fakeLog().log)).rejects.toThrow(/ev-2/)
    },
  )
})

describe('buildSubprojectLookup', () => {
  const lookup = buildSubprojectLookup([
    { id: 'seg-a', slug: 'alpha', sourceId: 'PROJ-A' },
    { id: 'seg-b1', slug: 'beta-one', sourceId: 'proj-b' },
    { id: 'seg-b2', slug: 'beta-two', sourceId: 'Proj-B' },
  ])

  it('resolves a project id case-insensitively, whatever the slug', () => {
    expect(lookup(null, 'proj-a')).toBe('seg-a')
    expect(lookup('unrelated', 'Proj-A')).toBe('seg-a')
  })

  it('resolves a project id held by several subprojects to null', () => {
    expect(lookup(null, 'PROJ-B')).toBeNull()
  })

  it('resolves a missing or unknown project id to null', () => {
    expect(lookup('alpha', null)).toBeNull()
    expect(lookup(null, 'proj-z')).toBeNull()
  })
})

function event(eventKey: string): SessionizeEventDecision['event'] {
  return {
    eventKey,
    eventName: `Fake Summit ${eventKey}`,
    eventStart: '2027-04-28',
    eventEnd: '2027-04-29',
    cfpStart: null,
    cfpEnd: null,
  }
}

function matchedDecision(eventKey: string, segmentId: string): SessionizeEventDecision {
  return {
    event: event(eventKey),
    status: 'matched',
    holdReason: null,
    cventEventId: `cvent-${eventKey}`,
    segmentId,
    rule: 'exact_name',
    ruleVersion: 1,
    evidence: { candidates: [] },
  }
}

function heldDecision(
  eventKey: string,
  holdReason: SessionizeEventDecision['holdReason'],
): SessionizeEventDecision {
  return {
    event: event(eventKey),
    status: 'held',
    holdReason,
    cventEventId: null,
    segmentId: null,
    rule: null,
    ruleVersion: 1,
    evidence: { candidates: [] },
  }
}

function toInput(decision: SessionizeEventDecision) {
  const { event: listed, ...rest } = decision
  return { ...listed, ...rest }
}

function storedRow(decision: SessionizeEventDecision): IStoredSessionizeEventMapping {
  return { ...toInput(decision), reviewedAt: '2026-11-01', lockedAt: null, deactivatedAt: null }
}

describe('applySessionizeDecisions', () => {
  const upsert = vi.mocked(upsertSessionizeEventDecision)
  const deactivate = vi.mocked(deactivateUnlistedSessionizeEvents)
  const list = vi.mocked(listSessionizeEventMappings)
  const qx = { tx: (fn: (tx: QueryExecutor) => unknown) => fn(qx) } as unknown as QueryExecutor

  beforeEach(() => {
    vi.resetAllMocks()
  })

  it('writes every decision, reports a reviewed row once, then deactivates the unlisted ones', async () => {
    const outcomes: Record<string, SessionizeDecisionOutcome> = {
      new: 'inserted',
      same: 'unchanged',
      reviewed: 'protected',
      fresh: 'inserted',
    }
    upsert.mockImplementation(async (_tx, input) => outcomes[input.eventKey])
    deactivate.mockResolvedValue(1)
    list.mockResolvedValue([
      storedRow(matchedDecision('reviewed', 'seg-1')),
      storedRow(heldDecision('same', 'ambiguous')),
    ])
    const { warn, log } = fakeLog()
    const decisions = [
      matchedDecision('new', 'seg-1'),
      heldDecision('same', 'ambiguous'),
      heldDecision('reviewed', 'no_segment'),
      heldDecision('fresh', 'no_match'),
    ]

    const summary = await applySessionizeDecisions(qx, decisions, log)

    expect(summary).toEqual({
      listed: 4,
      matched: 1,
      held: { no_match: 1, ambiguous: 1, no_segment: 1 },
      outcomes: { inserted: 2, updated: 0, unchanged: 1, protected: 1 },
      deactivated: 1,
    })
    expect(upsert.mock.calls.map(([, input]) => input)).toEqual(decisions.map(toInput))
    expect(warn).toHaveBeenCalledTimes(1)
    expect(warn.mock.calls[0][0]).toEqual({
      event: 'sessionize_decision_needs_review',
      eventKey: 'reviewed',
      wanted: { status: 'held', holdReason: 'no_segment', segmentId: null, cventEventId: null },
      stored: {
        status: 'matched',
        holdReason: null,
        segmentId: 'seg-1',
        cventEventId: 'cvent-reviewed',
      },
    })
    expect(deactivate).toHaveBeenCalledTimes(1)
    expect(deactivate).toHaveBeenCalledWith(qx, ['new', 'same', 'reviewed', 'fresh'])
    expect(deactivate.mock.invocationCallOrder[0]).toBeGreaterThan(
      Math.max(...upsert.mock.invocationCallOrder),
    )
  })

  it('refuses an empty listing and writes nothing', async () => {
    list.mockResolvedValue([])
    deactivate.mockRejectedValue(new Error('empty listing'))

    await expect(applySessionizeDecisions(qx, [], fakeLog().log)).rejects.toThrow('empty listing')

    expect(upsert).not.toHaveBeenCalled()
  })
})

function consoleRow(key: string, name: string, dates: string): string {
  return `<tr><td>b</td><td><a href="/app/organizer/event/${key}">${name}</a></td><td></td><td>${dates}<div>soon</div></td><td><div>x</div></td><td></td></tr>`
}

function stubHttp(rows: string[]): ConnectorHttp {
  const page = `<div id="tab-activated" class="tab-pane"><table><tbody>${rows.join('')}</tbody></table></div><div id="tab-test"></div>`
  return {
    request: vi.fn().mockResolvedValue(page),
    requestCount: vi.fn(),
  } as unknown as ConnectorHttp
}

describe('computeSessionizeDecisions', () => {
  const segments = vi.mocked(listActiveSubprojectSegmentSources)
  const qx = {} as QueryExecutor

  beforeEach(() => {
    vi.resetAllMocks()
  })

  it('matches every activated event against the catalogue and the subprojects', async () => {
    segments.mockResolvedValue([{ id: 'seg-1', slug: 'fake-one', sourceId: 'proj-1' }])
    const http = stubHttp([
      consoleRow('101', 'Fake Summit 2027', '15 Mar 2027'),
      consoleRow('102', 'Fake Unknown Day', '20 Mar 2027'),
    ])
    const catalogue = stubSource([
      {
        eventId: 'ev-1',
        eventName: 'Fake Summit 2027',
        startDate: '2027-03-16',
        projectId: 'PROJ-1',
      },
    ])

    const decisions = await computeSessionizeDecisions({
      http,
      catalogue,
      qx,
      log: fakeLog().log,
    })

    expect(http.request).toHaveBeenCalledTimes(1)
    expect(catalogue.queries).toHaveLength(1)
    expect(segments).toHaveBeenCalledWith(qx)
    expect(
      decisions.map(({ event, status, holdReason, cventEventId, segmentId }) => ({
        eventKey: event.eventKey,
        status,
        holdReason,
        cventEventId,
        segmentId,
      })),
    ).toEqual([
      {
        eventKey: '101',
        status: 'matched',
        holdReason: null,
        cventEventId: 'ev-1',
        segmentId: 'seg-1',
      },
      {
        eventKey: '102',
        status: 'held',
        holdReason: 'no_match',
        cventEventId: null,
        segmentId: null,
      },
    ])
  })
})
