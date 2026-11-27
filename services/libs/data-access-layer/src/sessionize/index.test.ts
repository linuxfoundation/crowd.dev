import { test as base, describe, expect } from 'vitest'

import { ConflictError, DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'

import { listActiveSubprojectSegmentSources, listActiveSubprojectsForMapping } from '../segments'
import {
  ISessionizeEventDecisionInput,
  confirmSessionizeEventMapping,
  confirmSessionizeEventMappings,
  deactivateUnlistedSessionizeEvents,
  findOrCreateSessionizeIntegration,
  getSessionizeEvent,
  ignoreSessionizeEvent,
  listOnboardableSessionizeEvents,
  listSessionizeEventMappings,
  listSessionizeEventsForReview,
  markSessionizeEventOnboarded,
  restoreSessionizeEvent,
  upsertSessionizeEventDecision,
} from './index'

const test = withQx(base)

interface ISegmentFixture {
  slug?: string
  name?: string
  parentSlug?: string | null
  grandparentSlug?: string | null
  status?: string
  sourceId?: string | null
}

async function createSegment(qx: QueryExecutor, fixture: ISegmentFixture = {}): Promise<string> {
  const id = generateUUIDv1()
  const slug = fixture.slug ?? `segment-${id}`
  await qx.result(
    `
    INSERT INTO public.segments
      (id, slug, name, "parentSlug", "grandparentSlug", status, "sourceId", "tenantId",
       "createdAt", "updatedAt")
    VALUES ($(id), $(slug), $(name), $(parentSlug), $(grandparentSlug), $(status), $(sourceId),
            $(tenantId), NOW(), NOW())
    `,
    {
      id,
      slug,
      name: fixture.name ?? slug,
      parentSlug: fixture.parentSlug ?? null,
      grandparentSlug: 'grandparentSlug' in fixture ? fixture.grandparentSlug : 'lf',
      status: fixture.status ?? 'active',
      sourceId: 'sourceId' in fixture ? fixture.sourceId : `source-${id}`,
      tenantId: DEFAULT_TENANT_ID,
    },
  )
  return id
}

async function readMapping(
  qx: QueryExecutor,
  eventKey: string,
): Promise<{
  eventName: string
  status: string
  holdReason: string | null
  cventEventId: string | null
  catalogueSource: string | null
  segmentId: string | null
  evidence: Record<string, unknown>
  integrationId: string | null
  lockedAt: string | null
  deactivatedAt: string | null
  updatedAt: string
  lastSeenAt: string
}> {
  return qx.selectOne(
    `
    SELECT "eventName", status, "holdReason", "cventEventId", "catalogueSource", "segmentId", evidence,
           "integrationId", "lockedAt", "deactivatedAt", "updatedAt", "lastSeenAt"
    FROM integration."sessionizeEventMappings"
    WHERE "eventKey" = $(eventKey)
    `,
    { eventKey },
  )
}

const BACKDATED = '2020-01-01T00:00:00Z'

async function backdateMapping(qx: QueryExecutor, eventKey: string): Promise<void> {
  await qx.result(
    `
    UPDATE integration."sessionizeEventMappings"
    SET "updatedAt" = $(backdated), "lastSeenAt" = $(backdated)
    WHERE "eventKey" = $(eventKey)
    `,
    { eventKey, backdated: BACKDATED },
  )
}

async function markMapping(
  qx: QueryExecutor,
  eventKey: string,
  column: 'reviewedAt' | 'lockedAt',
): Promise<void> {
  await qx.result(
    `UPDATE integration."sessionizeEventMappings" SET $(column:name) = now() WHERE "eventKey" = $(eventKey)`,
    { eventKey, column },
  )
}

async function archiveSegment(qx: QueryExecutor, segmentId: string): Promise<void> {
  await qx.result(`UPDATE public.segments SET status = 'archived' WHERE id = $(segmentId)`, {
    segmentId,
  })
}

function millis(timestamp: string): number {
  return new Date(timestamp).getTime()
}

function matched(eventKey: string, segmentId: string): ISessionizeEventDecisionInput {
  return {
    eventKey,
    eventName: `Open Source Summit ${eventKey}`,
    eventStart: '2027-04-28',
    eventEnd: '2027-04-29',
    cfpStart: '2026-12-01 09:00:00',
    cfpEnd: null,
    status: 'matched',
    holdReason: null,
    cventEventId: `cvent-${eventKey}`,
    catalogueSource: 'cvent',
    segmentId,
    rule: 'exact_name',
    ruleVersion: 1,
    evidence: { cventEventId: `cvent-${eventKey}`, dayGap: 0 },
  }
}

function held(eventKey: string): ISessionizeEventDecisionInput {
  return {
    ...matched(eventKey, ''),
    status: 'held',
    holdReason: 'ambiguous',
    cventEventId: null,
    catalogueSource: null,
    segmentId: null,
    rule: null,
    evidence: { candidates: 2 },
  }
}

describe('sessionize event decisions', () => {
  test('inserts a decision and lists it as stored', async ({ qx }) => {
    const segmentId = await createSegment(qx)
    const decision = matched('1001', segmentId)

    expect(await upsertSessionizeEventDecision(qx, decision)).toBe('inserted')

    expect(await listSessionizeEventMappings(qx)).toEqual([
      { ...decision, reviewedAt: null, lockedAt: null, deactivatedAt: null },
    ])
  })

  test('a repeated decision only moves lastSeenAt', async ({ qx }) => {
    const decision = matched('1002', await createSegment(qx))
    await upsertSessionizeEventDecision(qx, decision)
    await backdateMapping(qx, '1002')

    expect(
      await upsertSessionizeEventDecision(qx, {
        ...decision,
        evidence: { dayGap: 0, cventEventId: 'cvent-1002' },
      }),
    ).toBe('unchanged')

    const row = await readMapping(qx, '1002')
    expect(millis(row.updatedAt)).toBe(millis(BACKDATED))
    expect(millis(row.lastSeenAt)).toBeGreaterThan(millis(BACKDATED))
  })

  test('a changed decision rewrites the row', async ({ qx }) => {
    await upsertSessionizeEventDecision(qx, matched('1003', await createSegment(qx)))
    await backdateMapping(qx, '1003')

    expect(await upsertSessionizeEventDecision(qx, held('1003'))).toBe('updated')

    const row = await readMapping(qx, '1003')
    expect(row).toMatchObject({
      status: 'held',
      holdReason: 'ambiguous',
      cventEventId: null,
      segmentId: null,
      evidence: { candidates: 2 },
    })
    expect(millis(row.updatedAt)).toBeGreaterThan(millis(BACKDATED))
  })

  test.for([
    ['reviewedAt', 'unchanged'],
    ['lockedAt', 'protected'],
  ] as const)(
    'a row with %s keeps its target and reports a held decision as %s',
    async ([column, heldOutcome], { qx }) => {
      const segmentId = await createSegment(qx)
      await upsertSessionizeEventDecision(qx, matched('1004', segmentId))
      await markMapping(qx, '1004', column)
      await deactivateUnlistedSessionizeEvents(qx, ['other'])
      const stored = (await listSessionizeEventMappings(qx)).find((row) => row.eventKey === '1004')
      expect(typeof stored?.[column]).toBe('string')
      expect(typeof stored?.deactivatedAt).toBe('string')

      expect(
        await upsertSessionizeEventDecision(qx, { ...held('1004'), eventName: 'Renamed Summit' }),
      ).toBe(heldOutcome)
      expect(
        await upsertSessionizeEventDecision(qx, {
          ...matched('1004', await createSegment(qx)),
          eventName: 'Renamed Summit',
        }),
      ).toBe('protected')
      expect(
        await upsertSessionizeEventDecision(qx, {
          ...matched('1004', segmentId),
          eventName: 'Renamed Summit',
          evidence: { dayGap: 3 },
        }),
      ).toBe('unchanged')

      expect(await readMapping(qx, '1004')).toMatchObject({
        eventName: 'Renamed Summit',
        status: 'matched',
        holdReason: null,
        cventEventId: 'cvent-1004',
        segmentId,
        evidence: { cventEventId: 'cvent-1004', dayGap: 0 },
        deactivatedAt: null,
      })
    },
  )

  test('deactivates unlisted events and reactivates one that reappears', async ({ qx }) => {
    const segmentId = await createSegment(qx)
    await upsertSessionizeEventDecision(qx, matched('1005', segmentId))
    await upsertSessionizeEventDecision(qx, held('1006'))

    const { id: integrationId } = await findOrCreateSessionizeIntegration(qx, segmentId)
    await markSessionizeEventOnboarded(qx, '1005', segmentId, integrationId)

    expect(await deactivateUnlistedSessionizeEvents(qx, ['1006'])).toEqual([
      { eventKey: '1005', integrationId },
    ])
    expect(await deactivateUnlistedSessionizeEvents(qx, ['1006'])).toEqual([])
    expect((await readMapping(qx, '1005')).deactivatedAt).not.toBeNull()
    expect((await readMapping(qx, '1006')).deactivatedAt).toBeNull()

    expect(await upsertSessionizeEventDecision(qx, matched('1005', segmentId))).toBe('unchanged')
    expect((await readMapping(qx, '1005')).deactivatedAt).toBeNull()
  })

  test('refuses to deactivate against an empty listing', async ({ qx }) => {
    await upsertSessionizeEventDecision(qx, held('1007'))

    await expect(deactivateUnlistedSessionizeEvents(qx, [])).rejects.toThrow()
    expect((await readMapping(qx, '1007')).deactivatedAt).toBeNull()
  })
})

describe('sessionize onboarding', () => {
  test('creates the integration only on a subproject segment', async ({ qx }) => {
    const project = await createSegment(qx, { grandparentSlug: null })

    await expect(findOrCreateSessionizeIntegration(qx, project)).rejects.toThrow(/subproject/)
    await expect(findOrCreateSessionizeIntegration(qx, generateUUIDv1())).rejects.toThrow(
      /subproject/,
    )
    expect(
      await qx.select(`SELECT id FROM public.integrations WHERE platform = 'sessionize'`),
    ).toEqual([])

    const subproject = await createSegment(qx)
    expect(await findOrCreateSessionizeIntegration(qx, subproject)).toMatchObject({
      created: true,
      restored: false,
    })
  })

  test('lists the live matched events in key order', async ({ qx }) => {
    const segmentId = await createSegment(qx)
    await upsertSessionizeEventDecision(qx, matched('2003', segmentId))
    await upsertSessionizeEventDecision(qx, matched('2001', segmentId))
    await upsertSessionizeEventDecision(qx, held('2002'))
    await upsertSessionizeEventDecision(qx, matched('2004', segmentId))
    await deactivateUnlistedSessionizeEvents(qx, ['2001', '2002', '2003'])
    const { id: integrationId } = await findOrCreateSessionizeIntegration(qx, segmentId)
    await markSessionizeEventOnboarded(qx, '2003', segmentId, integrationId)

    expect(await listOnboardableSessionizeEvents(qx)).toEqual([
      {
        eventKey: '2001',
        eventName: 'Open Source Summit 2001',
        segmentId,
        integrationId: null,
      },
      { eventKey: '2003', eventName: 'Open Source Summit 2003', segmentId, integrationId },
    ])

    await archiveSegment(qx, segmentId)
    expect(await listOnboardableSessionizeEvents(qx)).toEqual([
      { eventKey: '2003', eventName: 'Open Source Summit 2003', segmentId, integrationId },
    ])
  })

  test('marks an event onboarded once and leaves a marked row untouched', async ({ qx }) => {
    const segmentId = await createSegment(qx)
    await upsertSessionizeEventDecision(qx, matched('2005', segmentId))
    await backdateMapping(qx, '2005')
    const { id: integrationId } = await findOrCreateSessionizeIntegration(qx, segmentId)

    await markSessionizeEventOnboarded(qx, '2005', segmentId, integrationId)
    const marked = await readMapping(qx, '2005')
    expect(marked.integrationId).toBe(integrationId)
    expect(typeof marked.lockedAt).toBe('string')
    expect(millis(marked.updatedAt)).toBeGreaterThan(millis(BACKDATED))

    await backdateMapping(qx, '2005')
    await markSessionizeEventOnboarded(qx, '2005', segmentId, integrationId)
    const again = await readMapping(qx, '2005')
    expect(again.lockedAt).toBe(marked.lockedAt)
    expect(millis(again.updatedAt)).toBe(millis(BACKDATED))

    const otherSegment = await createSegment(qx)
    await upsertSessionizeEventDecision(qx, held('2006'))
    await upsertSessionizeEventDecision(qx, matched('2007', segmentId))
    await deactivateUnlistedSessionizeEvents(qx, ['2005', '2006'])
    for (const [eventKey, segment] of [
      ['unknown-event', segmentId],
      ['2005', otherSegment],
      ['2006', segmentId],
      ['2007', segmentId],
    ]) {
      await expect(
        markSessionizeEventOnboarded(qx, eventKey, segment, integrationId),
      ).rejects.toThrow(new RegExp(eventKey))
    }
    expect((await readMapping(qx, '2007')).integrationId).toBeNull()
  })
})

describe('getSessionizeEvent', () => {
  test('reads the event by key whatever its status, and null for an unknown key', async ({
    qx,
  }) => {
    await upsertSessionizeEventDecision(qx, held('1008'))
    await deactivateUnlistedSessionizeEvents(qx, ['other'])

    expect(await getSessionizeEvent(qx, '1008')).toEqual({
      eventKey: '1008',
      eventName: 'Open Source Summit 1008',
      eventStart: '2027-04-28',
      eventEnd: '2027-04-29',
    })
    expect(await getSessionizeEvent(qx, 'unknown-event')).toBeNull()
  })
})

describe('listActiveSubprojectSegmentSources', () => {
  test('returns active subprojects that carry a source id', async ({ qx }) => {
    const subproject = await createSegment(qx, { sourceId: 'pcc-1' })
    const created = [
      subproject,
      await createSegment(qx, { grandparentSlug: null }),
      await createSegment(qx, { status: 'archived' }),
      await createSegment(qx, { sourceId: null }),
    ]

    const sources = (await listActiveSubprojectSegmentSources(qx)).filter((row) =>
      created.includes(row.id),
    )

    expect(sources).toEqual([{ id: subproject, sourceId: 'pcc-1' }])
  })
})

async function readReview(
  qx: QueryExecutor,
  eventKey: string,
): Promise<{ rule: string | null; reviewedBy: string | null; reviewedAt: string | null }> {
  return qx.selectOne(
    `
    SELECT rule, "reviewedBy", "reviewedAt"
    FROM integration."sessionizeEventMappings"
    WHERE "eventKey" = $(eventKey)
    `,
    { eventKey },
  )
}

const REVIEWER = 'reviewer@example.org'

function confirmInput(eventKey: string, segmentId: string) {
  return {
    eventKey,
    segmentId,
    cventEventId: `cvent-${eventKey}`,
    catalogueSource: 'cvent',
    reviewedBy: REVIEWER,
  }
}

describe('sessionize event review', () => {
  test('a confirmed held event is onboardable and protected from the daily run', async ({ qx }) => {
    const segmentId = await createSegment(qx)
    await upsertSessionizeEventDecision(qx, held('3001'))

    await confirmSessionizeEventMapping(qx, confirmInput('3001', segmentId))

    expect(await listOnboardableSessionizeEvents(qx)).toEqual([
      { eventKey: '3001', eventName: 'Open Source Summit 3001', segmentId, integrationId: null },
    ])
    expect(await readMapping(qx, '3001')).toMatchObject({
      status: 'matched',
      holdReason: 'ambiguous',
      cventEventId: 'cvent-3001',
      catalogueSource: 'cvent',
      segmentId,
    })
    expect(await readReview(qx, '3001')).toMatchObject({ rule: 'review', reviewedBy: REVIEWER })
    expect(await upsertSessionizeEventDecision(qx, held('3001'))).toBe('unchanged')
    expect(await upsertSessionizeEventDecision(qx, matched('3001', segmentId))).toBe('unchanged')
    expect(await upsertSessionizeEventDecision(qx, matched('3001', await createSegment(qx)))).toBe(
      'protected',
    )
    expect(await readMapping(qx, '3001')).toMatchObject({
      status: 'matched',
      holdReason: 'ambiguous',
      cventEventId: 'cvent-3001',
      segmentId,
    })

    await archiveSegment(qx, segmentId)
    expect(await listOnboardableSessionizeEvents(qx)).toEqual([])
    expect(await upsertSessionizeEventDecision(qx, held('3001'))).toBe('protected')
  })

  test('confirm refuses a segment that is not an active subproject', async ({ qx }) => {
    await upsertSessionizeEventDecision(qx, held('3002'))
    const project = await createSegment(qx, { grandparentSlug: null })
    const archived = await createSegment(qx, { status: 'archived' })

    for (const segmentId of [project, archived, generateUUIDv1()]) {
      await expect(
        confirmSessionizeEventMapping(qx, confirmInput('3002', segmentId)),
      ).rejects.toBeInstanceOf(ConflictError)
    }
    const subproject = await createSegment(qx)
    for (const pair of [
      { cventEventId: 'cvent-3002', catalogueSource: null },
      { cventEventId: null, catalogueSource: 'cvent' },
    ]) {
      await expect(
        confirmSessionizeEventMapping(qx, { ...confirmInput('3002', subproject), ...pair }),
      ).rejects.toThrow(/catalogueSource/)
    }
    expect(await readMapping(qx, '3002')).toMatchObject({ status: 'held', segmentId: null })
  })

  test('confirm and ignore refuse a row that is not an unreviewed live hold', async ({ qx }) => {
    const segmentId = await createSegment(qx)
    await upsertSessionizeEventDecision(qx, held('3003'))
    await confirmSessionizeEventMapping(qx, confirmInput('3003', segmentId))
    await upsertSessionizeEventDecision(qx, matched('3004', segmentId))
    await upsertSessionizeEventDecision(qx, held('3005'))
    await markMapping(qx, '3005', 'reviewedAt')
    await upsertSessionizeEventDecision(qx, held('3006'))
    await deactivateUnlistedSessionizeEvents(qx, ['3003', '3004', '3005'])

    for (const eventKey of ['3003', '3004', '3005', '3006', 'unknown-event']) {
      await expect(
        confirmSessionizeEventMapping(qx, confirmInput(eventKey, segmentId)),
      ).rejects.toBeInstanceOf(ConflictError)
      await expect(
        ignoreSessionizeEvent(qx, { eventKey, reviewedBy: REVIEWER }),
      ).rejects.toBeInstanceOf(ConflictError)
    }
    expect((await readMapping(qx, '3004')).status).toBe('matched')
    expect((await readMapping(qx, '3005')).status).toBe('held')
    expect((await readMapping(qx, '3006')).status).toBe('held')
  })

  test('an ignored event is not onboardable and stays ignored', async ({ qx }) => {
    await upsertSessionizeEventDecision(qx, held('3007'))

    await ignoreSessionizeEvent(qx, { eventKey: '3007', reviewedBy: REVIEWER })

    expect(await listOnboardableSessionizeEvents(qx)).toEqual([])
    expect(await readMapping(qx, '3007')).toMatchObject({
      status: 'excluded',
      holdReason: 'ambiguous',
      segmentId: null,
    })
    expect(await readReview(qx, '3007')).toMatchObject({ rule: null, reviewedBy: REVIEWER })
    expect(await upsertSessionizeEventDecision(qx, held('3007'))).toBe('unchanged')
    expect(await upsertSessionizeEventDecision(qx, matched('3007', await createSegment(qx)))).toBe(
      'protected',
    )
    await expect(
      ignoreSessionizeEvent(qx, { eventKey: '3007', reviewedBy: REVIEWER }),
    ).rejects.toBeInstanceOf(ConflictError)
  })

  test('restore puts a reviewed event back on hold for the daily run', async ({ qx }) => {
    const segmentId = await createSegment(qx)
    await upsertSessionizeEventDecision(qx, {
      ...held('3008'),
      holdReason: 'no_segment',
      cventEventId: 'cvent-3008',
      catalogueSource: 'lfx',
    })
    await upsertSessionizeEventDecision(qx, held('3009'))
    await ignoreSessionizeEvent(qx, { eventKey: '3008', reviewedBy: REVIEWER })
    await confirmSessionizeEventMapping(qx, confirmInput('3009', segmentId))

    for (const [eventKey, holdReason, cventEventId, catalogueSource] of [
      ['3008', 'no_segment', 'cvent-3008', 'lfx'],
      ['3009', 'ambiguous', null, null],
    ] as const) {
      await restoreSessionizeEvent(qx, eventKey)
      expect(await readMapping(qx, eventKey)).toMatchObject({
        status: 'held',
        holdReason,
        cventEventId,
        catalogueSource,
        segmentId: null,
      })
      expect(await readReview(qx, eventKey)).toEqual({
        rule: null,
        reviewedBy: null,
        reviewedAt: null,
      })
    }
    expect(await listOnboardableSessionizeEvents(qx)).toEqual([])
    expect(await upsertSessionizeEventDecision(qx, matched('3008', segmentId))).toBe('updated')
  })

  test('restore refuses an onboarded, unreviewed or never held row', async ({ qx }) => {
    const segmentId = await createSegment(qx)
    await upsertSessionizeEventDecision(qx, held('3010'))
    await confirmSessionizeEventMapping(qx, confirmInput('3010', segmentId))
    const { id: integrationId } = await findOrCreateSessionizeIntegration(qx, segmentId)
    await markSessionizeEventOnboarded(qx, '3010', segmentId, integrationId)
    await upsertSessionizeEventDecision(qx, held('3011'))
    await upsertSessionizeEventDecision(qx, matched('3012', segmentId))
    await markMapping(qx, '3012', 'reviewedAt')

    for (const eventKey of ['3010', '3011', '3012', 'unknown-event']) {
      await expect(restoreSessionizeEvent(qx, eventKey)).rejects.toBeInstanceOf(ConflictError)
    }
    expect((await readMapping(qx, '3010')).status).toBe('matched')
    expect((await readMapping(qx, '3012')).status).toBe('matched')
  })

  test('lists held and reviewed events with their segment and project group', async ({ qx }) => {
    const groupSlug = `group-${generateUUIDv1()}`
    await createSegment(qx, { slug: groupSlug, name: 'Made Up Group', grandparentSlug: null })
    await createSegment(qx, {
      slug: groupSlug,
      name: 'Made Up Project',
      parentSlug: groupSlug,
      grandparentSlug: null,
    })
    const segmentId = await createSegment(qx, {
      name: 'Made Up Subproject',
      parentSlug: groupSlug,
      grandparentSlug: groupSlug,
    })
    await upsertSessionizeEventDecision(qx, { ...held('3013'), eventStart: '2027-01-10' })
    await upsertSessionizeEventDecision(qx, { ...held('3014'), eventStart: '2027-03-10' })
    await upsertSessionizeEventDecision(qx, { ...held('3015'), eventStart: '2027-03-10' })
    await upsertSessionizeEventDecision(qx, matched('3016', segmentId))
    await upsertSessionizeEventDecision(qx, held('3017'))
    await deactivateUnlistedSessionizeEvents(qx, ['3013', '3014', '3015', '3016'])
    await confirmSessionizeEventMapping(qx, confirmInput('3014', segmentId))
    await ignoreSessionizeEvent(qx, { eventKey: '3015', reviewedBy: REVIEWER })

    const listed = await listSessionizeEventsForReview(qx)

    expect(listed.map((row) => row.eventKey)).toEqual(['3014', '3015', '3013'])
    expect(listed[0]).toEqual({
      eventKey: '3014',
      eventName: 'Open Source Summit 3014',
      eventStart: '2027-03-10',
      eventEnd: '2027-04-29',
      status: 'matched',
      holdReason: 'ambiguous',
      cventEventId: 'cvent-3014',
      segmentId,
      segmentName: 'Made Up Subproject',
      projectGroupName: 'Made Up Group',
      evidence: { candidates: 2 },
      reviewedAt: expect.any(String),
      lockedAt: null,
    })
    expect(listed[1]).toMatchObject({
      status: 'excluded',
      segmentId: null,
      segmentName: null,
      projectGroupName: null,
      reviewedAt: expect.any(String),
    })
    expect(listed[2]).toMatchObject({ status: 'held', reviewedAt: null })
  })
})

describe('listActiveSubprojectsForMapping', () => {
  test('returns active subprojects with their project group name', async ({ qx }) => {
    const groupSlug = `group-${generateUUIDv1()}`
    await createSegment(qx, { slug: groupSlug, name: 'Made Up Group', grandparentSlug: null })
    const subproject = await createSegment(qx, {
      slug: `sub-${groupSlug}`,
      name: 'Made Up Subproject',
      parentSlug: groupSlug,
      grandparentSlug: groupSlug,
      sourceId: 'pcc-made-up',
    })
    const withoutSource = await createSegment(qx, {
      slug: `bare-${groupSlug}`,
      name: 'Bare Subproject',
      parentSlug: groupSlug,
      grandparentSlug: groupSlug,
      sourceId: null,
    })
    const excluded = [
      await createSegment(qx, { grandparentSlug: null, parentSlug: groupSlug }),
      await createSegment(qx, { status: 'archived', grandparentSlug: groupSlug }),
    ]

    const listed = (await listActiveSubprojectsForMapping(qx)).filter((row) =>
      [subproject, withoutSource, ...excluded].includes(row.id),
    )

    expect(listed).toEqual([
      {
        id: withoutSource,
        name: 'Bare Subproject',
        slug: `bare-${groupSlug}`,
        sourceId: null,
        groupName: 'Made Up Group',
      },
      {
        id: subproject,
        name: 'Made Up Subproject',
        slug: `sub-${groupSlug}`,
        sourceId: 'pcc-made-up',
        groupName: 'Made Up Group',
      },
    ])
  })
})

describe('confirmSessionizeEventMappings', () => {
  test('confirms every event in one transaction', async ({ qx }) => {
    const segmentId = await createSegment(qx)
    for (const eventKey of ['3101', '3102']) {
      await upsertSessionizeEventDecision(qx, held(eventKey))
    }

    await confirmSessionizeEventMappings(qx, [
      confirmInput('3102', segmentId),
      confirmInput('3101', segmentId),
    ])

    for (const eventKey of ['3101', '3102']) {
      expect(await readMapping(qx, eventKey)).toMatchObject({
        status: 'matched',
        segmentId,
        cventEventId: `cvent-${eventKey}`,
        catalogueSource: 'cvent',
      })
    }
  })

  test('refuses a batch that names an event twice before changing anything', async ({ qx }) => {
    const segmentId = await createSegment(qx)
    await upsertSessionizeEventDecision(qx, held('3106'))

    await expect(
      confirmSessionizeEventMappings(qx, [
        confirmInput('3106', segmentId),
        confirmInput('3106', segmentId),
      ]),
    ).rejects.toThrow(/only once/)
    expect(await readMapping(qx, '3106')).toMatchObject({ status: 'held', segmentId: null })
  })

  test('a conflict on a later event rolls back the ones before it', async ({ qx }) => {
    const segmentId = await createSegment(qx)
    for (const eventKey of ['3103', '3104', '3105']) {
      await upsertSessionizeEventDecision(qx, held(eventKey))
    }
    await ignoreSessionizeEvent(qx, { eventKey: '3105', reviewedBy: REVIEWER })

    await expect(
      confirmSessionizeEventMappings(qx, [
        confirmInput('3103', segmentId),
        confirmInput('3104', segmentId),
        confirmInput('3105', segmentId),
      ]),
    ).rejects.toBeInstanceOf(ConflictError)

    for (const eventKey of ['3103', '3104']) {
      expect(await readMapping(qx, eventKey)).toMatchObject({ status: 'held', segmentId: null })
      expect(await readReview(qx, eventKey)).toEqual({
        rule: null,
        reviewedBy: null,
        reviewedAt: null,
      })
    }
  })
})
