import { test as base, describe, expect } from 'vitest'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'

import { findOrCreateCredlyIntegration } from './index'

const test = withQx(base)

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

async function readIntegration(
  qx: QueryExecutor,
  id: string,
): Promise<{ platform: string; segmentId: string; status: string; deletedAt: Date | null }> {
  return qx.selectOne(
    `SELECT platform, "segmentId", status, "deletedAt" FROM public.integrations WHERE id = $(id)`,
    { id },
  )
}

describe('findOrCreateCredlyIntegration', () => {
  test('creates the integration on the segment once and reuses it afterwards', async ({ qx }) => {
    const segmentId = await createSegment(qx)

    const first = await findOrCreateCredlyIntegration(qx, segmentId)
    const second = await findOrCreateCredlyIntegration(qx, segmentId)

    expect(first).toEqual({ id: first.id, created: true, restored: false })
    expect(second).toEqual({ id: first.id, created: false, restored: false })
    expect(await readIntegration(qx, first.id)).toEqual({
      platform: 'credly',
      segmentId,
      status: 'done',
      deletedAt: null,
    })
  })

  test('restores a soft-deleted integration instead of creating a second one', async ({ qx }) => {
    const segmentId = await createSegment(qx)
    const { id } = await findOrCreateCredlyIntegration(qx, segmentId)
    await qx.result(`UPDATE public.integrations SET "deletedAt" = NOW() WHERE id = $(id)`, { id })

    const restored = await findOrCreateCredlyIntegration(qx, segmentId)

    expect(restored).toEqual({ id, created: false, restored: true })
    expect((await readIntegration(qx, id)).deletedAt).toBeNull()
  })

  test('keeps integrations of different segments apart', async ({ qx }) => {
    const first = await findOrCreateCredlyIntegration(qx, await createSegment(qx))
    const second = await findOrCreateCredlyIntegration(qx, await createSegment(qx))

    expect(second.created).toBe(true)
    expect(second.id).not.toBe(first.id)
  })
})
