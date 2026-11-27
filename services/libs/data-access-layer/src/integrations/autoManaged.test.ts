import { test as base, describe, expect } from 'vitest'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'
import { PlatformType } from '@crowd/types'

import { findOrCreateAutoManagedIntegration } from './autoManaged'

const test = withQx(base)

async function createSegment(qx: QueryExecutor): Promise<string> {
  const id = generateUUIDv1()
  await qx.result(
    `
    INSERT INTO public.segments
      (id, slug, name, "grandparentSlug", status, "tenantId", "createdAt", "updatedAt")
    VALUES ($(id), $(id), $(id), 'fake-group', 'active', $(tenantId), NOW(), NOW())
    `,
    { id, tenantId: DEFAULT_TENANT_ID },
  )
  return id
}

async function listIntegrations(
  qx: QueryExecutor,
  segmentId: string,
): Promise<{ id: string; platform: string; status: string; deletedAt: string | null }[]> {
  return qx.select(
    `SELECT id, platform, status, "deletedAt" FROM public.integrations WHERE "segmentId" = $(segmentId)`,
    { segmentId },
  )
}

async function holdsAdvisoryLock(tx: QueryExecutor, key: string): Promise<boolean> {
  const row = await tx.selectOne(
    `
    SELECT EXISTS (
      SELECT 1
      FROM pg_locks
      WHERE locktype = 'advisory'
        AND pid = pg_backend_pid()
        AND ((classid::bigint << 32) | objid::bigint) = hashtextextended($(key), 0)
    ) AS held
    `,
    { key },
  )
  return row.held
}

describe('findOrCreateAutoManagedIntegration', () => {
  test('creates one integration on the segment and reuses it afterwards', async ({ qx }) => {
    const segmentId = await createSegment(qx)

    const first = await findOrCreateAutoManagedIntegration(qx, PlatformType.SESSIONIZE, segmentId)
    const second = await findOrCreateAutoManagedIntegration(qx, PlatformType.SESSIONIZE, segmentId)

    expect(first).toEqual({ id: first.id, created: true, restored: false })
    expect(second).toEqual({ id: first.id, created: false, restored: false })
    expect(await listIntegrations(qx, segmentId)).toEqual([
      { id: first.id, platform: 'sessionize', status: 'done', deletedAt: null },
    ])
  })

  test('restores a soft-deleted integration instead of creating a second one', async ({ qx }) => {
    const segmentId = await createSegment(qx)
    const { id } = await findOrCreateAutoManagedIntegration(qx, PlatformType.SESSIONIZE, segmentId)
    await qx.result(`UPDATE public.integrations SET "deletedAt" = NOW() WHERE id = $(id)`, { id })

    expect(
      await findOrCreateAutoManagedIntegration(qx, PlatformType.SESSIONIZE, segmentId),
    ).toEqual({ id, created: false, restored: true })
    expect(await listIntegrations(qx, segmentId)).toEqual([
      { id, platform: 'sessionize', status: 'done', deletedAt: null },
    ])
  })

  test('throws on two live integrations of the platform on the segment', async ({ qx }) => {
    const segmentId = await createSegment(qx)
    for (let i = 0; i < 2; i++) {
      await qx.result(
        `
        INSERT INTO public.integrations
          (id, platform, status, "segmentId", "tenantId", "createdAt", "updatedAt")
        VALUES ($(id), 'sessionize', 'done', $(segmentId), $(tenantId), NOW(), NOW())
        `,
        { id: generateUUIDv1(), segmentId, tenantId: DEFAULT_TENANT_ID },
      )
    }

    await expect(
      findOrCreateAutoManagedIntegration(qx, PlatformType.SESSIONIZE, segmentId),
    ).rejects.toThrow(/2 live sessionize integrations/)
  })

  test.for([
    [PlatformType.CREDLY, 'credlyIntegration'],
    [PlatformType.SESSIONIZE, 'sessionizeIntegration'],
  ] as const)('locks %s integrations on the key %s', async ([platform, key], { qx }) => {
    const segmentId = await createSegment(qx)

    const held = await qx.tx(async (tx) => {
      await findOrCreateAutoManagedIntegration(tx, platform, segmentId)
      return holdsAdvisoryLock(tx, key)
    })

    expect(held).toBe(true)
  })
})
