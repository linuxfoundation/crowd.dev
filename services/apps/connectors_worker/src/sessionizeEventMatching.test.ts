import { test as base, describe, expect, it, vi } from 'vitest'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { SessionizeEventDecision } from '@crowd/connectors/src/connectors/sessionize/matching'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import {
  listSessionizeEventMappings,
  upsertSessionizeEventDecision,
} from '@crowd/data-access-layer/src/sessionize'
import type { Logger } from '@crowd/logging'
import { withQx } from '@crowd/test-kit/db'

import type { CatalogueSource } from './credlyTemplateResolve'
import {
  applySessionizeDecisions,
  buildSubprojectLookup,
  loadCventCatalogue,
} from './sessionizeEventMatching'

const test = withQx(base)

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

    expect(await loadCventCatalogue(source)).toEqual([
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
    await expect(loadCventCatalogue(stubSource([]))).rejects.toThrow(/empty/)
  })

  it.for(['2027-02-30', '2027-4-28', '28/04/2027'])(
    'fails on a start date that is not a calendar date (%s)',
    async (startDate) => {
      const source = stubSource([
        { eventId: 'ev-1', eventName: 'Fake Summit', startDate: '2027-04-28', projectId: null },
        { eventId: 'ev-2', eventName: 'Fake Day', startDate, projectId: null },
      ])
      await expect(loadCventCatalogue(source)).rejects.toThrow(/ev-2/)
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

async function createSegment(qx: QueryExecutor): Promise<string> {
  const id = generateUUIDv1()
  await qx.result(
    `
    INSERT INTO public.segments
      (id, slug, name, "grandparentSlug", status, "sourceId", "tenantId", "createdAt", "updatedAt")
    VALUES ($(id), $(slug), $(slug), 'fake-foundation', 'active', $(sourceId), $(tenantId),
            NOW(), NOW())
    `,
    { id, slug: `segment-${id}`, sourceId: `source-${id}`, tenantId: DEFAULT_TENANT_ID },
  )
  return id
}

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

describe('applySessionizeDecisions', () => {
  test('writes every decision, reports a reviewed row once and deactivates an unlisted one', async ({
    qx,
  }) => {
    const segmentId = await createSegment(qx)
    await upsertSessionizeEventDecision(qx, toInput(matchedDecision('reviewed', segmentId)))
    await qx.result(
      `UPDATE integration."sessionizeEventMappings" SET "reviewedAt" = now() WHERE "eventKey" = 'reviewed'`,
    )
    await upsertSessionizeEventDecision(qx, toInput(heldDecision('gone', 'no_match')))
    await upsertSessionizeEventDecision(qx, toInput(heldDecision('same', 'ambiguous')))
    const warn = vi.fn()
    const log = { warn, info: vi.fn() } as unknown as Logger

    const summary = await applySessionizeDecisions(
      qx,
      [
        matchedDecision('new', segmentId),
        heldDecision('same', 'ambiguous'),
        heldDecision('reviewed', 'no_segment'),
        heldDecision('fresh', 'no_match'),
      ],
      log,
    )

    expect(summary).toEqual({
      listed: 4,
      matched: 1,
      held: { no_match: 1, ambiguous: 1, no_segment: 1 },
      outcomes: { inserted: 2, updated: 0, unchanged: 1, protected: 1 },
      deactivated: 1,
    })
    expect(warn).toHaveBeenCalledTimes(1)
    expect(warn.mock.calls[0][0]).toEqual({
      event: 'sessionize_decision_needs_review',
      eventKey: 'reviewed',
      wanted: { status: 'held', holdReason: 'no_segment', segmentId: null, cventEventId: null },
      stored: {
        status: 'matched',
        holdReason: null,
        segmentId,
        cventEventId: 'cvent-reviewed',
      },
    })
    const rows = await listSessionizeEventMappings(qx)
    expect(
      Object.fromEntries(rows.map((row) => [row.eventKey, row.deactivatedAt !== null])),
    ).toEqual({ fresh: false, gone: true, new: false, reviewed: false, same: false })
    expect(rows.find((row) => row.eventKey === 'reviewed')).toMatchObject({
      status: 'matched',
      segmentId,
    })
  })

  test('refuses an empty listing and writes nothing', async ({ qx }) => {
    await upsertSessionizeEventDecision(qx, toInput(heldDecision('kept', 'no_match')))
    const log = { warn: vi.fn(), info: vi.fn() } as unknown as Logger

    await expect(applySessionizeDecisions(qx, [], log)).rejects.toThrow()

    const rows = await listSessionizeEventMappings(qx)
    expect(rows.map((row) => [row.eventKey, row.deactivatedAt])).toEqual([['kept', null]])
  })
})
