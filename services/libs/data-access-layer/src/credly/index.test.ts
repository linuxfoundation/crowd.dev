import { test as base, describe, expect } from 'vitest'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'

import {
  deactivateCredlyTemplateMapping,
  findOrCreateCredlyIntegration,
  listActiveCredlyTemplateMappings,
  upsertCredlyTemplateMapping,
} from './index'

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

async function readMapping(
  qx: QueryExecutor,
  badgeTemplateId: string,
): Promise<{
  integrationId: string
  segmentId: string
  badgeTemplateName: string
  deactivatedAt: Date | null
  updatedAt: Date
  lastSeenAt: Date
}> {
  return qx.selectOne(
    `
    SELECT "integrationId", "segmentId", "badgeTemplateName", "deactivatedAt", "updatedAt", "lastSeenAt"
    FROM integration."credlyTemplateMappings"
    WHERE "badgeTemplateId" = $(badgeTemplateId)
    `,
    { badgeTemplateId },
  )
}

async function mappedProject(
  qx: QueryExecutor,
): Promise<{ segmentId: string; integrationId: string }> {
  const segmentId = await createSegment(qx)
  const { id: integrationId } = await findOrCreateCredlyIntegration(qx, segmentId)
  return { segmentId, integrationId }
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

describe('credly template mappings', () => {
  test('upsert inserts a mapping and a repeat only moves lastSeenAt', async ({ qx }) => {
    const { segmentId, integrationId } = await mappedProject(qx)
    const mapping = {
      badgeTemplateId: 'tpl-cka',
      badgeTemplateName: 'CKA: Certified Kubernetes Administrator',
      integrationId,
      segmentId,
    }

    await upsertCredlyTemplateMapping(qx, mapping)
    const inserted = await readMapping(qx, 'tpl-cka')
    await upsertCredlyTemplateMapping(qx, mapping)
    const repeated = await readMapping(qx, 'tpl-cka')

    expect(inserted).toMatchObject({ integrationId, segmentId, deactivatedAt: null })
    expect(repeated.updatedAt).toEqual(inserted.updatedAt)
    expect(repeated.lastSeenAt.getTime()).toBeGreaterThanOrEqual(inserted.lastSeenAt.getTime())
  })

  test('upsert moves a template to another project and reactivates a deactivated one', async ({
    qx,
  }) => {
    const first = await mappedProject(qx)
    const second = await mappedProject(qx)
    const name = 'ICA: Istio Certified Associate'
    await upsertCredlyTemplateMapping(qx, {
      badgeTemplateId: 'tpl-ica',
      badgeTemplateName: name,
      ...first,
    })
    const inserted = await readMapping(qx, 'tpl-ica')
    expect(await deactivateCredlyTemplateMapping(qx, 'tpl-ica')).toBe(1)
    expect((await readMapping(qx, 'tpl-ica')).deactivatedAt).not.toBeNull()

    await upsertCredlyTemplateMapping(qx, {
      badgeTemplateId: 'tpl-ica',
      badgeTemplateName: name,
      ...second,
    })

    const moved = await readMapping(qx, 'tpl-ica')
    expect(moved).toMatchObject({ ...second, deactivatedAt: null })
    expect(moved.updatedAt.getTime()).toBeGreaterThan(inserted.updatedAt.getTime())
    expect(await deactivateCredlyTemplateMapping(qx, 'tpl-unknown')).toBe(0)
  })

  test('upsert refuses a pair that is not a live Credly integration on that segment', async ({
    qx,
  }) => {
    const { integrationId } = await mappedProject(qx)
    const otherSegmentId = await createSegment(qx)

    await expect(
      upsertCredlyTemplateMapping(qx, {
        badgeTemplateId: 'tpl-mismatch',
        badgeTemplateName: 'tpl-mismatch',
        integrationId,
        segmentId: otherSegmentId,
      }),
    ).rejects.toThrow('not a live Credly integration')
  })

  test('listActive skips deactivated mappings and those whose integration is gone', async ({
    qx,
  }) => {
    const live = await mappedProject(qx)
    const deactivated = await mappedProject(qx)
    const deleted = await mappedProject(qx)
    for (const [badgeTemplateId, project] of [
      ['tpl-live', live],
      ['tpl-deactivated', deactivated],
      ['tpl-deleted', deleted],
    ] as const) {
      await upsertCredlyTemplateMapping(qx, {
        badgeTemplateId,
        badgeTemplateName: badgeTemplateId,
        ...project,
      })
    }
    await deactivateCredlyTemplateMapping(qx, 'tpl-deactivated')
    await qx.result(`UPDATE public.integrations SET "deletedAt" = NOW() WHERE id = $(id)`, {
      id: deleted.integrationId,
    })

    const active = await listActiveCredlyTemplateMappings(qx)

    expect(active.filter((m) => m.badgeTemplateId.startsWith('tpl-'))).toEqual([
      { badgeTemplateId: 'tpl-live', badgeTemplateName: 'tpl-live', ...live },
    ])
  })
})
