import { test as base, describe, expect } from 'vitest'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'

import {
  deactivateCredlyTemplateMapping,
  findOrCreateCredlyIntegration,
  listActiveCredlyTemplateMappings,
  listCredlyTemplateMappings,
  listHeldCredlyTemplateMappings,
  listStaleCredlyTemplateMappings,
  lockCredlyTemplateMapping,
  upsertCredlyTemplateDecision,
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
  integrationId: string | null
  segmentId: string | null
  badgeTemplateName: string
  status: string
  holdReason: string | null
  projectSlug: string | null
  badgeCount: number | null
  rule: string | null
  evidence: Record<string, unknown> | null
  reviewedBy: string | null
  reviewedAt: string | null
  lockedAt: string | null
  deactivatedAt: string | null
  updatedAt: string
  lastSeenAt: string
}> {
  return qx.selectOne(
    `
    SELECT "integrationId", "segmentId", "badgeTemplateName", status, "holdReason", "projectSlug",
           "badgeCount", rule, evidence, "reviewedBy", "reviewedAt", "lockedAt", "deactivatedAt",
           "updatedAt", "lastSeenAt"
    FROM integration."credlyTemplateMappings"
    WHERE "badgeTemplateId" = $(badgeTemplateId)
    `,
    { badgeTemplateId },
  )
}

const BACKDATED = '2020-01-01T00:00:00Z'

async function backdateMapping(qx: QueryExecutor, badgeTemplateId: string): Promise<void> {
  await qx.result(
    `
    UPDATE integration."credlyTemplateMappings"
    SET "updatedAt" = $(backdated), "lastSeenAt" = $(backdated)
    WHERE "badgeTemplateId" = $(badgeTemplateId)
    `,
    { badgeTemplateId, backdated: BACKDATED },
  )
}

function millis(timestamp: string): number {
  return new Date(timestamp).getTime()
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
  const mapped = (badgeTemplateId: string, project: { segmentId: string; integrationId: string }) =>
    ({
      badgeTemplateId,
      badgeTemplateName: badgeTemplateId,
      status: 'mapped',
      rule: 'exact_sku',
      ruleVersion: 1,
      ...project,
    }) as const

  test('a mapped decision is inserted once and a repeat only moves lastSeenAt and badgeCount', async ({
    qx,
  }) => {
    const project = await mappedProject(qx)
    const decision = {
      ...mapped('tpl-cka', project),
      badgeTemplateName: 'CKA: Certified Kubernetes Administrator',
      matchedSku: 'CKA',
      evidence: { sku: 'CKA' },
    }

    expect(await upsertCredlyTemplateDecision(qx, { ...decision, badgeCount: 10 })).toBe('inserted')
    const inserted = await readMapping(qx, 'tpl-cka')
    await backdateMapping(qx, 'tpl-cka')
    expect(await upsertCredlyTemplateDecision(qx, { ...decision, badgeCount: 12 })).toBe(
      'unchanged',
    )
    const repeated = await readMapping(qx, 'tpl-cka')

    expect(inserted).toMatchObject({
      ...project,
      status: 'mapped',
      rule: 'exact_sku',
      badgeCount: 10,
      evidence: { sku: 'CKA' },
      lockedAt: null,
      deactivatedAt: null,
    })
    expect(repeated.badgeCount).toBe(12)
    expect(millis(repeated.updatedAt)).toBe(millis(BACKDATED))
    expect(millis(repeated.lastSeenAt)).toBeGreaterThan(millis(BACKDATED))
  })

  test('a decision moves a template to another project and reactivates a deactivated one', async ({
    qx,
  }) => {
    const first = await mappedProject(qx)
    const second = await mappedProject(qx)
    await upsertCredlyTemplateDecision(qx, mapped('tpl-ica', first))
    expect(await deactivateCredlyTemplateMapping(qx, 'tpl-ica')).toBe(1)
    expect((await readMapping(qx, 'tpl-ica')).deactivatedAt).not.toBeNull()
    await backdateMapping(qx, 'tpl-ica')

    expect(await upsertCredlyTemplateDecision(qx, mapped('tpl-ica', second))).toBe('updated')

    const moved = await readMapping(qx, 'tpl-ica')
    expect(moved).toMatchObject({ ...second, deactivatedAt: null })
    expect(millis(moved.updatedAt)).toBeGreaterThan(millis(BACKDATED))
    expect(await deactivateCredlyTemplateMapping(qx, 'tpl-unknown')).toBe(0)
  })

  test('evidence keys left undefined compare like keys the database never stored', async ({
    qx,
  }) => {
    const project = await mappedProject(qx)
    const base = mapped('tpl-undef', project)
    await upsertCredlyTemplateDecision(qx, { ...base, evidence: { sku: 'CKA', group: undefined } })

    expect(
      await upsertCredlyTemplateDecision(qx, {
        ...base,
        evidence: { sku: 'CKA', group: undefined },
      }),
    ).toBe('unchanged')
    expect(await upsertCredlyTemplateDecision(qx, { ...base, evidence: { sku: 'CKA' } })).toBe(
      'unchanged',
    )
  })

  test('a locked template cannot be deactivated', async ({ qx }) => {
    const project = await mappedProject(qx)
    await upsertCredlyTemplateDecision(qx, mapped('tpl-lock-keep', project))
    await lockCredlyTemplateMapping(qx, 'tpl-lock-keep', project)

    expect(await deactivateCredlyTemplateMapping(qx, 'tpl-lock-keep')).toBe(0)
    expect((await readMapping(qx, 'tpl-lock-keep')).deactivatedAt).toBeNull()
  })

  test('the full list returns every row with its lock and deactivation state', async ({ qx }) => {
    const project = await mappedProject(qx)
    await upsertCredlyTemplateDecision(qx, mapped('tpl-all-a', project))
    await upsertCredlyTemplateDecision(qx, {
      badgeTemplateId: 'tpl-all-b',
      badgeTemplateName: 'tpl-all-b',
      status: 'held',
      holdReason: 'no_match',
    })
    await lockCredlyTemplateMapping(qx, 'tpl-all-a', project)
    await deactivateCredlyTemplateMapping(qx, 'tpl-all-b')

    const rows = (await listCredlyTemplateMappings(qx)).filter((m) =>
      m.badgeTemplateId.startsWith('tpl-all-'),
    )

    expect(
      rows.map((m) => [m.badgeTemplateId, m.status, m.lockedAt !== null, m.deactivatedAt !== null]),
    ).toEqual([
      ['tpl-all-a', 'mapped', true, false],
      ['tpl-all-b', 'held', false, true],
    ])
  })

  test('evidence with the same content in another key order leaves the decision unchanged', async ({
    qx,
  }) => {
    const project = await mappedProject(qx)
    const base = mapped('tpl-evidence', project)
    await upsertCredlyTemplateDecision(qx, {
      ...base,
      evidence: { sku: 'CKA', catalogue: { group: 'k8s', variants: ['CKA-CN', 'CKA-JP'] } },
    })

    expect(
      await upsertCredlyTemplateDecision(qx, {
        ...base,
        evidence: { catalogue: { variants: ['CKA-CN', 'CKA-JP'], group: 'k8s' }, sku: 'CKA' },
      }),
    ).toBe('unchanged')
    expect(
      await upsertCredlyTemplateDecision(qx, {
        ...base,
        evidence: { sku: 'CKA', catalogue: { group: 'k8s', variants: ['CKA-JP', 'CKA-CN'] } },
      }),
    ).toBe('updated')
  })

  test('the same decision reactivates a deactivated template', async ({ qx }) => {
    const project = await mappedProject(qx)
    await upsertCredlyTemplateDecision(qx, mapped('tpl-back', project))
    await deactivateCredlyTemplateMapping(qx, 'tpl-back')

    expect(await upsertCredlyTemplateDecision(qx, mapped('tpl-back', project))).toBe('updated')

    expect((await readMapping(qx, 'tpl-back')).deactivatedAt).toBeNull()
  })

  test('a decision with an inconsistent status is refused before touching the table', async ({
    qx,
  }) => {
    await expect(
      upsertCredlyTemplateDecision(qx, {
        badgeTemplateId: 'tpl-bad',
        badgeTemplateName: 'tpl-bad',
        status: 'mapped',
      }),
    ).rejects.toThrow('needs a segment and an integration')
    await expect(
      upsertCredlyTemplateDecision(qx, {
        badgeTemplateId: 'tpl-bad',
        badgeTemplateName: 'tpl-bad',
        status: 'held',
      }),
    ).rejects.toThrow('needs a hold reason')
  })

  test('a mapped decision refuses a pair that is not a live Credly integration on that segment', async ({
    qx,
  }) => {
    const { integrationId } = await mappedProject(qx)
    const otherSegmentId = await createSegment(qx)

    await expect(
      upsertCredlyTemplateDecision(
        qx,
        mapped('tpl-mismatch', { integrationId, segmentId: otherSegmentId }),
      ),
    ).rejects.toThrow('not a live Credly integration')
  })

  test('a held template keeps its reason and project slug without any integration', async ({
    qx,
  }) => {
    expect(
      await upsertCredlyTemplateDecision(qx, {
        badgeTemplateId: 'tpl-held',
        badgeTemplateName: 'LFS114: free5GC',
        badgeCount: 2000,
        status: 'held',
        holdReason: 'no_segment',
        projectSlug: 'free5gc',
        rule: 'hold',
        ruleVersion: 1,
      }),
    ).toBe('inserted')

    expect(await readMapping(qx, 'tpl-held')).toMatchObject({
      status: 'held',
      holdReason: 'no_segment',
      projectSlug: 'free5gc',
      integrationId: null,
      segmentId: null,
    })
    expect(
      await lockCredlyTemplateMapping(qx, 'tpl-held', {
        segmentId: generateUUIDv1(),
        integrationId: generateUUIDv1(),
      }),
    ).toBe(false)
  })

  test('a reviewer override moves a held template to a project and records who decided', async ({
    qx,
  }) => {
    const project = await mappedProject(qx)
    await upsertCredlyTemplateDecision(qx, {
      badgeTemplateId: 'tpl-lfc131',
      badgeTemplateName: 'LFC131: Green Software for Practitioners',
      status: 'held',
      holdReason: 'no_project',
    })

    expect(
      await upsertCredlyTemplateDecision(qx, {
        ...mapped('tpl-lfc131', project),
        rule: 'override',
        reviewedBy: 'joana',
      }),
    ).toBe('updated')

    const released = await readMapping(qx, 'tpl-lfc131')
    expect(released).toMatchObject({
      ...project,
      status: 'mapped',
      holdReason: null,
      rule: 'override',
    })
    expect(released.reviewedBy).toBe('joana')
    expect(released.reviewedAt).not.toBeNull()

    await upsertCredlyTemplateDecision(qx, { ...mapped('tpl-lfc131', project), rule: 'title' })

    const automatic = await readMapping(qx, 'tpl-lfc131')
    expect(automatic.rule).toBe('title')
    expect(automatic.reviewedBy).toBeNull()
    expect(automatic.reviewedAt).toBeNull()
  })

  test('a locked template is not moved by a new decision and reports it', async ({ qx }) => {
    const first = await mappedProject(qx)
    const second = await mappedProject(qx)
    await upsertCredlyTemplateDecision(qx, mapped('tpl-locked', first))

    expect(await lockCredlyTemplateMapping(qx, 'tpl-locked', second)).toBe(false)
    expect(await lockCredlyTemplateMapping(qx, 'tpl-locked', first)).toBe(true)
    expect(await lockCredlyTemplateMapping(qx, 'tpl-locked', first)).toBe(false)
    expect(await upsertCredlyTemplateDecision(qx, mapped('tpl-locked', second))).toBe('locked')
    expect(
      await upsertCredlyTemplateDecision(qx, {
        ...mapped('tpl-locked', first),
        badgeTemplateName: 'renamed',
      }),
    ).toBe('updated')

    const kept = await readMapping(qx, 'tpl-locked')
    expect(kept).toMatchObject({ ...first, status: 'mapped', badgeTemplateName: 'renamed' })
    expect(kept.lockedAt).not.toBeNull()
  })

  test('the held queue is ordered by badge count and leaves mapped and excluded templates out', async ({
    qx,
  }) => {
    const project = await mappedProject(qx)
    await upsertCredlyTemplateDecision(qx, mapped('tpl-q-mapped', project))
    await upsertCredlyTemplateDecision(qx, {
      badgeTemplateId: 'tpl-q-excluded',
      badgeTemplateName: 'LFF101: ELearning Badge Test',
      status: 'excluded',
    })
    for (const [badgeTemplateId, badgeCount, holdReason] of [
      ['tpl-q-small', 5, 'no_match'],
      ['tpl-q-big', 87000, 'no_project'],
      ['tpl-q-unknown', null, 'ambiguous'],
    ] as const) {
      await upsertCredlyTemplateDecision(qx, {
        badgeTemplateId,
        badgeTemplateName: badgeTemplateId,
        badgeCount,
        status: 'held',
        holdReason,
      })
    }

    const held = await listHeldCredlyTemplateMappings(qx)

    expect(
      held.filter((m) => m.badgeTemplateId.startsWith('tpl-q-')).map((m) => m.badgeTemplateId),
    ).toEqual(['tpl-q-big', 'tpl-q-small', 'tpl-q-unknown'])
    expect(held.find((m) => m.badgeTemplateId === 'tpl-q-big')).toMatchObject({
      holdReason: 'no_project',
      badgeCount: 87000,
    })
  })

  test('active and stale lists split mapped templates by whether their integration is live on the segment', async ({
    qx,
  }) => {
    const live = await mappedProject(qx)
    const deactivated = await mappedProject(qx)
    const deleted = await mappedProject(qx)
    const moved = await mappedProject(qx)
    for (const [badgeTemplateId, project] of [
      ['tpl-live', live],
      ['tpl-deactivated', deactivated],
      ['tpl-deleted', deleted],
      ['tpl-moved', moved],
    ] as const) {
      await upsertCredlyTemplateDecision(qx, mapped(badgeTemplateId, project))
    }
    await upsertCredlyTemplateDecision(qx, {
      badgeTemplateId: 'tpl-held-out',
      badgeTemplateName: 'tpl-held-out',
      status: 'held',
      holdReason: 'no_match',
    })
    await deactivateCredlyTemplateMapping(qx, 'tpl-deactivated')
    await qx.result(`UPDATE public.integrations SET "deletedAt" = NOW() WHERE id = $(id)`, {
      id: deleted.integrationId,
    })
    await qx.result(`UPDATE public.integrations SET "segmentId" = $(segmentId) WHERE id = $(id)`, {
      id: moved.integrationId,
      segmentId: await createSegment(qx),
    })

    const active = await listActiveCredlyTemplateMappings(qx)
    const stale = await listStaleCredlyTemplateMappings(qx)

    expect(active.filter((m) => m.badgeTemplateId.startsWith('tpl-'))).toEqual([
      { badgeTemplateId: 'tpl-live', badgeTemplateName: 'tpl-live', ...live, lockedAt: null },
    ])
    expect(stale.filter((m) => m.badgeTemplateId.startsWith('tpl-'))).toEqual([
      {
        badgeTemplateId: 'tpl-deleted',
        badgeTemplateName: 'tpl-deleted',
        ...deleted,
        lockedAt: null,
      },
      { badgeTemplateId: 'tpl-moved', badgeTemplateName: 'tpl-moved', ...moved, lockedAt: null },
    ])
  })
})
