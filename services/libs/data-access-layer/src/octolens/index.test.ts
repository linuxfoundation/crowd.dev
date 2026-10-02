import { test as base, describe, expect } from 'vitest'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'

import {
  addOctolensKeywordMapping,
  findOctolensKeywordMapping,
  listOctolensKeywordMappings,
  removeOctolensKeywordMapping,
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

describe('octolens keyword mappings', () => {
  test('addOctolensKeywordMapping + findOctolensKeywordMapping', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const segmentId = await createSegment(qx)

    const id = await addOctolensKeywordMapping(qx, {
      integrationId,
      segmentId,
      keywordId: 101,
      keyword: 'crowd.dev',
    })

    const mapping = await findOctolensKeywordMapping(qx, integrationId, 101)

    expect(mapping).toMatchObject({
      id,
      integrationId,
      segmentId,
      keywordId: 101,
      keyword: 'crowd.dev',
    })
  })

  test('findOctolensKeywordMapping returns null when no mapping exists', async ({ qx }) => {
    const integrationId = await createIntegration(qx)

    const mapping = await findOctolensKeywordMapping(qx, integrationId, 999)

    expect(mapping).toBeNull()
  })

  test('listOctolensKeywordMappings returns only mappings for the given integration', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    const otherIntegrationId = await createIntegration(qx)
    const segmentId = await createSegment(qx)

    await addOctolensKeywordMapping(qx, {
      integrationId,
      segmentId,
      keywordId: 1,
      keyword: 'alpha',
    })
    await addOctolensKeywordMapping(qx, {
      integrationId,
      segmentId,
      keywordId: 2,
      keyword: 'beta',
    })
    await addOctolensKeywordMapping(qx, {
      integrationId: otherIntegrationId,
      segmentId,
      keywordId: 3,
      keyword: 'gamma',
    })

    const mappings = await listOctolensKeywordMappings(qx, integrationId)

    expect(mappings).toHaveLength(2)
    expect(mappings.map((m) => m.keyword).sort()).toEqual(['alpha', 'beta'])
  })

  test('addOctolensKeywordMapping rejects a duplicate (integrationId, keywordId) pair', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    const segmentId = await createSegment(qx)

    await addOctolensKeywordMapping(qx, {
      integrationId,
      segmentId,
      keywordId: 42,
      keyword: 'first',
    })

    await expect(
      addOctolensKeywordMapping(qx, {
        integrationId,
        segmentId,
        keywordId: 42,
        keyword: 'second',
      }),
    ).rejects.toThrow()
  })

  test('removeOctolensKeywordMapping deletes the mapping', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const segmentId = await createSegment(qx)

    await addOctolensKeywordMapping(qx, {
      integrationId,
      segmentId,
      keywordId: 7,
      keyword: 'delete-me',
    })

    await removeOctolensKeywordMapping(qx, integrationId, 7)

    const mapping = await findOctolensKeywordMapping(qx, integrationId, 7)
    expect(mapping).toBeNull()
  })

  test('removeOctolensKeywordMapping is a no-op when no mapping matches', async ({ qx }) => {
    const integrationId = await createIntegration(qx)

    await expect(removeOctolensKeywordMapping(qx, integrationId, 404)).resolves.toBeUndefined()
  })
})
