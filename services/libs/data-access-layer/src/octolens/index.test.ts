import { test as base, describe, expect } from 'vitest'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'

import {
  deactivateOctolensKeywordMappingsNotSeen,
  findOctolensKeywordMapping,
  listOctolensKeywordMappings,
  listOctolensKeywordMappingsDeactivatedBetween,
  listOctolensMappingDiscoveryStates,
  upsertOctolensKeywordMapping,
} from './index'

const test = withQx(base)

async function createIntegration(qx: QueryExecutor): Promise<string> {
  const id = generateUUIDv1()
  await qx.result(
    `
    INSERT INTO public.integrations (id, platform, status, "tenantId", "createdAt", "updatedAt", "deletedAt")
    VALUES ($(id), 'octolens', 'done', $(tenantId), NOW(), NOW(), NULL)
    `,
    { id, tenantId: DEFAULT_TENANT_ID },
  )
  return id
}

async function createSegment(qx: QueryExecutor): Promise<string> {
  const id = generateUUIDv1()
  const slug = `segment-${id}`
  await qx.result(
    `
    INSERT INTO public.segments (id, slug, name, "tenantId", "createdAt", "updatedAt")
    VALUES ($(id), $(slug), $(slug), $(tenantId), NOW(), NOW())
    `,
    { id, slug, tenantId: DEFAULT_TENANT_ID },
  )
  return id
}

async function readRowIncludingDeactivated(
  qx: QueryExecutor,
  integrationId: string,
  keywordId: number,
): Promise<{
  segmentId: string
  keyword: string
  deactivatedAt: Date | null
  updatedAt: Date
  lastSeenAt: Date
}> {
  return qx.selectOne(
    `
    SELECT "segmentId", keyword, "deactivatedAt", "updatedAt", "lastSeenAt"
    FROM integration."octolensKeywordMappings"
    WHERE "integrationId" = $(integrationId) AND "keywordId" = $(keywordId)
    `,
    { integrationId, keywordId },
  )
}

describe('octolens keyword mappings', () => {
  test('upsertOctolensKeywordMapping inserts a new mapping', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const segmentId = await createSegment(qx)

    await upsertOctolensKeywordMapping(qx, {
      integrationId,
      segmentId,
      keywordId: 101,
      keyword: 'crowd.dev',
    })

    const mapping = await findOctolensKeywordMapping(qx, integrationId, 101)

    expect(mapping).toMatchObject({
      integrationId,
      segmentId,
      keywordId: 101,
      keyword: 'crowd.dev',
    })
  })

  test('upsertOctolensKeywordMapping updates segment and keyword of an existing mapping', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    const segmentId = await createSegment(qx)
    const otherSegmentId = await createSegment(qx)

    await upsertOctolensKeywordMapping(qx, {
      integrationId,
      segmentId,
      keywordId: 5,
      keyword: 'old',
    })
    const before = await findOctolensKeywordMapping(qx, integrationId, 5)
    await qx.result(
      `
      UPDATE integration."octolensKeywordMappings" SET "updatedAt" = '2020-01-01T00:00:00Z'
      WHERE "integrationId" = $(integrationId)
      `,
      { integrationId },
    )

    await upsertOctolensKeywordMapping(qx, {
      integrationId,
      segmentId: otherSegmentId,
      keywordId: 5,
      keyword: 'new',
    })

    const after = await findOctolensKeywordMapping(qx, integrationId, 5)
    expect(after).toMatchObject({ id: before?.id, segmentId: otherSegmentId, keyword: 'new' })
    const row = await readRowIncludingDeactivated(qx, integrationId, 5)
    expect(new Date(row.updatedAt).getTime()).toBeGreaterThan(Date.parse('2020-01-02'))
  })

  test('upsertOctolensKeywordMapping on an unchanged mapping only moves lastSeenAt', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    const segmentId = await createSegment(qx)
    const data = { integrationId, segmentId, keywordId: 6, keyword: 'same' }

    await upsertOctolensKeywordMapping(qx, data)
    await qx.result(
      `
      UPDATE integration."octolensKeywordMappings"
      SET "updatedAt" = '2020-01-01T00:00:00Z', "lastSeenAt" = '2020-01-01T00:00:00Z'
      WHERE "integrationId" = $(integrationId)
      `,
      { integrationId },
    )

    await upsertOctolensKeywordMapping(qx, data)

    const row = await readRowIncludingDeactivated(qx, integrationId, 6)
    expect(new Date(row.updatedAt).toISOString()).toBe('2020-01-01T00:00:00.000Z')
    expect(new Date(row.lastSeenAt).getTime()).toBeGreaterThan(Date.parse('2020-01-02'))
  })

  test('upsertOctolensKeywordMapping reactivates a deactivated mapping', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const segmentId = await createSegment(qx)
    const data = { integrationId, segmentId, keywordId: 7, keyword: 'back' }

    await upsertOctolensKeywordMapping(qx, data)
    await upsertOctolensKeywordMapping(qx, { ...data, keywordId: 8, keyword: 'other' })
    await deactivateOctolensKeywordMappingsNotSeen(qx, integrationId, [8])
    expect(await findOctolensKeywordMapping(qx, integrationId, 7)).toBeNull()

    await upsertOctolensKeywordMapping(qx, data)

    expect(await findOctolensKeywordMapping(qx, integrationId, 7)).toMatchObject({
      keyword: 'back',
    })
    expect((await readRowIncludingDeactivated(qx, integrationId, 7)).deactivatedAt).toBeNull()
  })

  test('findOctolensKeywordMapping returns null when no mapping exists', async ({ qx }) => {
    const integrationId = await createIntegration(qx)

    const mapping = await findOctolensKeywordMapping(qx, integrationId, 999)

    expect(mapping).toBeNull()
  })

  test('listOctolensKeywordMappings returns only active mappings of the given integration', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    const otherIntegrationId = await createIntegration(qx)
    const segmentId = await createSegment(qx)

    for (const [keywordId, keyword] of [
      [1, 'alpha'],
      [2, 'beta'],
      [4, 'gone'],
    ] as const) {
      await upsertOctolensKeywordMapping(qx, { integrationId, segmentId, keywordId, keyword })
    }
    await upsertOctolensKeywordMapping(qx, {
      integrationId: otherIntegrationId,
      segmentId,
      keywordId: 3,
      keyword: 'gamma',
    })
    await deactivateOctolensKeywordMappingsNotSeen(qx, integrationId, [1, 2])

    const mappings = await listOctolensKeywordMappings(qx, integrationId)

    expect(mappings.map((m) => m.keyword)).toEqual(['alpha', 'beta'])
  })

  test('deactivateOctolensKeywordMappingsNotSeen only deactivates unseen mappings of the integration', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    const otherIntegrationId = await createIntegration(qx)
    const segmentId = await createSegment(qx)

    await upsertOctolensKeywordMapping(qx, {
      integrationId,
      segmentId,
      keywordId: 1,
      keyword: 'seen',
    })
    await upsertOctolensKeywordMapping(qx, {
      integrationId,
      segmentId,
      keywordId: 2,
      keyword: 'unseen',
    })
    await upsertOctolensKeywordMapping(qx, {
      integrationId: otherIntegrationId,
      segmentId,
      keywordId: 2,
      keyword: 'other integration',
    })

    const deactivated = await deactivateOctolensKeywordMappingsNotSeen(qx, integrationId, [1])

    expect(deactivated).toBe(1)
    expect(await findOctolensKeywordMapping(qx, integrationId, 1)).not.toBeNull()
    expect(await findOctolensKeywordMapping(qx, integrationId, 2)).toBeNull()
    expect(await findOctolensKeywordMapping(qx, otherIntegrationId, 2)).not.toBeNull()
  })

  test('deactivateOctolensKeywordMappingsNotSeen keeps the first deactivation timestamp', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    const segmentId = await createSegment(qx)

    await upsertOctolensKeywordMapping(qx, { integrationId, segmentId, keywordId: 1, keyword: 'a' })
    await upsertOctolensKeywordMapping(qx, { integrationId, segmentId, keywordId: 2, keyword: 'b' })
    await deactivateOctolensKeywordMappingsNotSeen(qx, integrationId, [1])
    const first = (await readRowIncludingDeactivated(qx, integrationId, 2)).deactivatedAt

    const deactivated = await deactivateOctolensKeywordMappingsNotSeen(qx, integrationId, [1])

    expect(deactivated).toBe(0)
    expect((await readRowIncludingDeactivated(qx, integrationId, 2)).deactivatedAt).toEqual(first)
  })

  test('deactivateOctolensKeywordMappingsNotSeen with no seen keywords deactivates nothing', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    const segmentId = await createSegment(qx)

    await upsertOctolensKeywordMapping(qx, {
      integrationId,
      segmentId,
      keywordId: 1,
      keyword: 'keep',
    })

    const deactivated = await deactivateOctolensKeywordMappingsNotSeen(qx, integrationId, [])

    expect(deactivated).toBe(0)
    expect(await findOctolensKeywordMapping(qx, integrationId, 1)).not.toBeNull()
  })
})

async function setMappingTimestamps(
  qx: QueryExecutor,
  integrationId: string,
  keywordId: number,
  timestamps: { lastSeenAt: string; deactivatedAt?: string },
): Promise<void> {
  await qx.result(
    `
    UPDATE integration."octolensKeywordMappings"
    SET "lastSeenAt" = $(lastSeenAt), "deactivatedAt" = $(deactivatedAt)
    WHERE "integrationId" = $(integrationId) AND "keywordId" = $(keywordId)
    `,
    {
      integrationId,
      keywordId,
      lastSeenAt: timestamps.lastSeenAt,
      deactivatedAt: timestamps.deactivatedAt ?? null,
    },
  )
}

describe('octolens mapping discovery health', () => {
  test('listOctolensMappingDiscoveryStates returns live octolens integrations without mappings', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    const deletedIntegrationId = await createIntegration(qx)
    await qx.result(`UPDATE public.integrations SET "deletedAt" = NOW() WHERE id = $(id)`, {
      id: deletedIntegrationId,
    })
    await qx.result(
      `
      INSERT INTO public.integrations (id, platform, status, "tenantId", "createdAt", "updatedAt")
      VALUES ($(id), 'github', 'done', $(tenantId), NOW(), NOW())
      `,
      { id: generateUUIDv1(), tenantId: DEFAULT_TENANT_ID },
    )

    const states = await listOctolensMappingDiscoveryStates(qx)

    expect(states).toHaveLength(1)
    expect(states[0]).toMatchObject({ integrationId, activeMappings: 0, lastDiscoveredAt: null })
    expect(states[0].integrationCreatedAt).toBeInstanceOf(Date)
  })

  test('listOctolensMappingDiscoveryStates counts active mappings and reports the oldest active lastSeenAt after a partial run', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    const segmentId = await createSegment(qx)
    for (const keywordId of [1, 2, 3]) {
      await upsertOctolensKeywordMapping(qx, {
        integrationId,
        segmentId,
        keywordId,
        keyword: `k${keywordId}`,
      })
    }
    await setMappingTimestamps(qx, integrationId, 1, { lastSeenAt: '2026-01-01T10:00:00Z' })
    await setMappingTimestamps(qx, integrationId, 2, { lastSeenAt: '2026-01-01T09:00:00Z' })
    await setMappingTimestamps(qx, integrationId, 3, {
      lastSeenAt: '2026-01-01T11:00:00Z',
      deactivatedAt: '2026-01-01T12:00:00Z',
    })

    const states = await listOctolensMappingDiscoveryStates(qx)

    expect(states).toHaveLength(1)
    expect(states[0]).toMatchObject({ integrationId, activeMappings: 2 })
    expect(states[0].lastDiscoveredAt?.toISOString()).toBe('2026-01-01T09:00:00.000Z')
    expect(states[0].checkedAt).toBeInstanceOf(Date)
  })

  test('listOctolensMappingDiscoveryStates falls back to the newest lastSeenAt without active mappings', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    const segmentId = await createSegment(qx)
    for (const keywordId of [1, 2]) {
      await upsertOctolensKeywordMapping(qx, {
        integrationId,
        segmentId,
        keywordId,
        keyword: `k${keywordId}`,
      })
    }
    await setMappingTimestamps(qx, integrationId, 1, {
      lastSeenAt: '2026-01-01T09:00:00Z',
      deactivatedAt: '2026-01-01T12:00:00Z',
    })
    await setMappingTimestamps(qx, integrationId, 2, {
      lastSeenAt: '2026-01-01T11:00:00Z',
      deactivatedAt: '2026-01-01T12:00:00Z',
    })

    const states = await listOctolensMappingDiscoveryStates(qx)

    expect(states).toHaveLength(1)
    expect(states[0]).toMatchObject({ integrationId, activeMappings: 0 })
    expect(states[0].lastDiscoveredAt?.toISOString()).toBe('2026-01-01T11:00:00.000Z')
  })

  test('listOctolensMappingDiscoveryStates skips deleted integrations', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const deletedIntegrationId = await createIntegration(qx)
    const segmentId = await createSegment(qx)
    for (const id of [integrationId, deletedIntegrationId]) {
      await upsertOctolensKeywordMapping(qx, {
        integrationId: id,
        segmentId,
        keywordId: 1,
        keyword: 'k',
      })
    }
    await qx.result(`UPDATE public.integrations SET "deletedAt" = NOW() WHERE id = $(id)`, {
      id: deletedIntegrationId,
    })

    const states = await listOctolensMappingDiscoveryStates(qx)

    expect(states.map((s) => s.integrationId)).toEqual([integrationId])
  })

  test('listOctolensKeywordMappingsDeactivatedBetween excludes since, includes until, joins the segment', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    const deletedIntegrationId = await createIntegration(qx)
    const segmentId = await createSegment(qx)
    const deactivatedAtByKeywordId: Record<number, string | undefined> = {
      1: '2026-01-01T10:00:00Z',
      2: '2026-01-01T10:30:00Z',
      3: '2026-01-01T11:00:00Z',
      4: '2026-01-01T11:00:01Z',
      5: undefined,
    }
    for (const [keywordId, deactivatedAt] of Object.entries(deactivatedAtByKeywordId)) {
      await upsertOctolensKeywordMapping(qx, {
        integrationId,
        segmentId,
        keywordId: Number(keywordId),
        keyword: `k${keywordId}`,
      })
      await setMappingTimestamps(qx, integrationId, Number(keywordId), {
        lastSeenAt: '2026-01-01T09:00:00Z',
        deactivatedAt,
      })
    }
    await upsertOctolensKeywordMapping(qx, {
      integrationId: deletedIntegrationId,
      segmentId,
      keywordId: 1,
      keyword: 'deleted',
    })
    await setMappingTimestamps(qx, deletedIntegrationId, 1, {
      lastSeenAt: '2026-01-01T09:00:00Z',
      deactivatedAt: '2026-01-01T10:30:00Z',
    })
    await qx.result(`UPDATE public.integrations SET "deletedAt" = NOW() WHERE id = $(id)`, {
      id: deletedIntegrationId,
    })

    const rows = await listOctolensKeywordMappingsDeactivatedBetween(
      qx,
      new Date('2026-01-01T10:00:00Z'),
      new Date('2026-01-01T11:00:00Z'),
    )

    expect(rows.map((r) => r.keyword)).toEqual(['k2', 'k3'])
    expect(rows[0]).toMatchObject({
      integrationId,
      keywordId: 2,
      segmentId,
      segmentName: `segment-${segmentId}`,
      segmentSlug: `segment-${segmentId}`,
    })
    expect(rows[0].deactivatedAt.toISOString()).toBe('2026-01-01T10:30:00.000Z')
  })
})
