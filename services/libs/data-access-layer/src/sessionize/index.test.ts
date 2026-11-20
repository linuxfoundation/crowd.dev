import { test as base, describe, expect } from 'vitest'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'

import { listActiveSubprojectSegmentSources } from '../segments'
import {
  ISessionizeEventDecisionInput,
  deactivateUnlistedSessionizeEvents,
  listSessionizeEventMappings,
  upsertSessionizeEventDecision,
} from './index'

const test = withQx(base)

interface ISegmentFixture {
  grandparentSlug?: string | null
  status?: string
  sourceId?: string | null
}

async function createSegment(qx: QueryExecutor, fixture: ISegmentFixture = {}): Promise<string> {
  const id = generateUUIDv1()
  const slug = `segment-${id}`
  await qx.result(
    `
    INSERT INTO public.segments
      (id, slug, name, "grandparentSlug", status, "sourceId", "tenantId", "createdAt", "updatedAt")
    VALUES ($(id), $(slug), $(slug), $(grandparentSlug), $(status), $(sourceId), $(tenantId),
            NOW(), NOW())
    `,
    {
      id,
      slug,
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
  segmentId: string | null
  evidence: Record<string, unknown>
  deactivatedAt: string | null
  updatedAt: string
  lastSeenAt: string
}> {
  return qx.selectOne(
    `
    SELECT "eventName", status, "holdReason", "cventEventId", "segmentId", evidence,
           "deactivatedAt", "updatedAt", "lastSeenAt"
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

  test.for(['reviewedAt', 'lockedAt'] as const)(
    'a row with %s keeps its target and reports the differing decision',
    async (column, { qx }) => {
      const segmentId = await createSegment(qx)
      await upsertSessionizeEventDecision(qx, matched('1004', segmentId))
      await markMapping(qx, '1004', column)
      await deactivateUnlistedSessionizeEvents(qx, ['other'])
      const stored = (await listSessionizeEventMappings(qx)).find((row) => row.eventKey === '1004')
      expect(typeof stored?.[column]).toBe('string')
      expect(typeof stored?.deactivatedAt).toBe('string')

      expect(
        await upsertSessionizeEventDecision(qx, { ...held('1004'), eventName: 'Renamed Summit' }),
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

    expect(await deactivateUnlistedSessionizeEvents(qx, ['1006'])).toBe(1)
    expect(await deactivateUnlistedSessionizeEvents(qx, ['1006'])).toBe(0)
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
