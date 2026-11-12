import { beforeEach, describe, expect, it, vi } from 'vitest'

import { fetchBadgeTemplatesPage } from '@crowd/connectors/src/connectors/credly/apiClient'
import type { ResolvedTemplate } from '@crowd/connectors/src/connectors/credly/mapping/resolver'
import type { ICredlyStoredTemplateMapping } from '@crowd/data-access-layer/src/credly'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import {
  applyDecisions,
  buildPlan,
  loadCatalogue,
  loadCredlyTemplates,
  loadSubprojectSegments,
} from './credlyTemplateResolve'
import { onboardCredlyTemplate } from './onboardCredlyTemplate'

function decision(id: string, overrides: Partial<ResolvedTemplate> = {}): ResolvedTemplate {
  return {
    badgeTemplateId: id,
    badgeTemplateName: id,
    badgeCount: null,
    status: 'mapped',
    holdReason: null,
    projectSlug: 'k8s',
    segmentId: 'seg-k8s',
    rule: 'exact_sku',
    ruleVersion: 1,
    matchedSku: 'CKA',
    evidence: {},
    reviewedBy: null,
    ...overrides,
  }
}

function stored(
  id: string,
  overrides: Partial<ICredlyStoredTemplateMapping> = {},
): ICredlyStoredTemplateMapping {
  return {
    badgeTemplateId: id,
    badgeTemplateName: id,
    status: 'mapped',
    holdReason: null,
    projectSlug: 'k8s',
    segmentId: 'seg-k8s',
    integrationId: 'int-k8s',
    rule: 'exact_sku',
    ruleVersion: 1,
    matchedSku: 'CKA',
    evidence: {},
    reviewedBy: null,
    lockedAt: null,
    deactivatedAt: null,
    ...overrides,
  }
}

describe('buildPlan', () => {
  it('classifies every decision against the stored rows and lists the rows Credly no longer has', () => {
    const plan = buildPlan(
      [
        decision('new'),
        decision('same'),
        decision('moved', { projectSlug: 'cncf', segmentId: 'seg-cncf' }),
        decision('locked-move', { projectSlug: 'cncf', segmentId: 'seg-cncf' }),
        decision('released', { status: 'mapped' }),
        decision('back'),
      ],
      [
        stored('same'),
        stored('moved'),
        stored('locked-move', { lockedAt: new Date() }),
        stored('released', {
          status: 'held',
          holdReason: 'no_project',
          segmentId: null,
          integrationId: null,
        }),
        stored('back', { deactivatedAt: new Date() }),
        stored('gone'),
        stored('gone-already', { deactivatedAt: new Date() }),
        stored('gone-locked', { lockedAt: new Date() }),
      ],
    )

    expect(plan.counts).toEqual({ new: 1, unchanged: 1, changed: 3, locked: 2, missing: 1 })
    expect(plan.entries.find((e) => e.badgeTemplateId === 'moved')).toEqual({
      kind: 'changed',
      badgeTemplateId: 'moved',
      badgeTemplateName: 'moved',
      before: 'mapped:k8s',
      after: 'mapped:cncf',
    })
    expect(plan.entries.find((e) => e.badgeTemplateId === 'locked-move')?.kind).toBe('locked')
    expect(plan.entries.find((e) => e.badgeTemplateId === 'released')?.before).toBe(
      'held:no_project',
    )
    expect(plan.entries.find((e) => e.badgeTemplateId === 'back')?.kind).toBe('changed')
    expect(plan.entries.find((e) => e.badgeTemplateId === 'gone')).toMatchObject({
      kind: 'missing',
      before: 'mapped:k8s',
      after: null,
    })
    expect(plan.entries.some((e) => e.badgeTemplateId === 'gone-already')).toBe(false)
    expect(plan.entries.find((e) => e.badgeTemplateId === 'gone-locked')).toMatchObject({
      kind: 'locked',
      after: null,
    })
  })

  it('reports a rule, version, evidence or reviewer change as changed even when the target stays', () => {
    const plan = buildPlan(
      [
        decision('v2', { ruleVersion: 2 }),
        decision('rule', { rule: 'title', matchedSku: null }),
        decision('evidence', { evidence: { votes: [{ projectSlug: 'k8s', enrollments: 2 }] } }),
        decision('reviewer', { reviewedBy: 'joana' }),
        decision('same', { evidence: { b: 1, a: 2 } }),
      ],
      [
        stored('v2'),
        stored('rule'),
        stored('evidence', { evidence: { votes: [{ projectSlug: 'k8s', enrollments: 1 }] } }),
        stored('reviewer'),
        stored('same', { evidence: { a: 2, b: 1 } }),
      ],
    )
    expect(plan.counts).toMatchObject({ changed: 4, unchanged: 1 })
  })

  it('keeps a held template unchanged when only the hold reason text differs in evidence', () => {
    const plan = buildPlan(
      [
        decision('held', {
          status: 'held',
          holdReason: 'no_match',
          segmentId: null,
          projectSlug: null,
        }),
      ],
      [
        stored('held', {
          status: 'held',
          holdReason: 'no_match',
          segmentId: null,
          integrationId: null,
          projectSlug: null,
        }),
      ],
    )
    expect(plan.counts.unchanged).toBe(1)
  })
})

describe('loadCatalogue', () => {
  it('runs the three catalogue queries and coerces the enrollment counts', async () => {
    const queries: string[] = []
    const source = {
      async run<T>(query: string): Promise<T[]> {
        queries.push(query)
        if (query.includes('bronze_census_ti.courses')) {
          return [{ sku: 'CKA', title: 'CKA', courseGroupId: 'g', isTestOrArchived: false }] as T[]
        }
        if (query.includes('_ti_course_groups_to_projects')) {
          return [{ courseGroupId: 'g', title: 'CKA', projectSlug: 'k8s' }] as T[]
        }
        return [{ courseCode: 'CKA', projectSlug: 'k8s', enrollments: '12' }] as T[]
      },
    }

    const catalogue = await loadCatalogue(source)

    expect(queries).toHaveLength(3)
    expect(catalogue.enrollments).toEqual([
      { courseCode: 'CKA', projectSlug: 'k8s', enrollments: 12 },
    ])
    expect(catalogue.courses[0].courseGroupId).toBe('g')
  })
})

describe('loadSubprojectSegments', () => {
  it('maps slugs case-insensitively and leaves out slugs shared by several segments', async () => {
    const warnings: unknown[] = []
    const qx = {
      select: async () => [
        { id: 'seg-1', slug: 'K8S' },
        { id: 'seg-2', slug: 'docs' },
        { id: 'seg-3', slug: 'Docs' },
      ],
    } as unknown as Parameters<typeof loadSubprojectSegments>[0]
    const log = { warn: (...args: unknown[]) => warnings.push(args) } as unknown as Parameters<
      typeof loadSubprojectSegments
    >[1]

    const segments = await loadSubprojectSegments(qx, log)

    expect([...segments]).toEqual([['k8s', 'seg-1']])
    expect(warnings).toHaveLength(1)
  })
})

vi.mock('@crowd/connectors/src/connectors/credly/apiClient', () => ({
  fetchBadgeTemplatesPage: vi.fn(),
}))

describe('loadCredlyTemplates', () => {
  const log = {} as Parameters<typeof loadCredlyTemplates>[2]
  const http = {} as Parameters<typeof loadCredlyTemplates>[0]
  const page = (
    data: { id: string; name: string; badges_count?: unknown; global_activity_url?: string }[],
    total_count: number,
  ) =>
    ({
      data,
      metadata: { count: data.length, current_page: 1, total_count, total_pages: 2 },
    }) as never

  it('reads every page and reports the listing complete when totals and ids line up', async () => {
    vi.mocked(fetchBadgeTemplatesPage)
      .mockResolvedValueOnce(
        page(
          [
            {
              id: 'a',
              name: 'A',
              badges_count: 3,
              global_activity_url: 'https://events.linuxfoundation.org/kubecon/',
            },
          ],
          2,
        ),
      )
      .mockResolvedValueOnce(page([{ id: 'b', name: 'B', badges_count: null }], 2))

    const listing = await loadCredlyTemplates(http, 'org', log)

    expect(listing.complete).toBe(true)
    expect(listing.templates).toEqual([
      {
        id: 'a',
        name: 'A',
        badgeCount: 3,
        url: null,
        activityUrl: 'https://events.linuxfoundation.org/kubecon/',
        createdAt: null,
      },
      { id: 'b', name: 'B', badgeCount: null, url: null, activityUrl: null, createdAt: null },
    ])
  })

  it('reports an incomplete listing when a template shows up twice across pages', async () => {
    vi.mocked(fetchBadgeTemplatesPage)
      .mockResolvedValueOnce(page([{ id: 'a', name: 'A' }], 2))
      .mockResolvedValueOnce(page([{ id: 'a', name: 'A' }], 2))

    expect((await loadCredlyTemplates(http, 'org', log)).complete).toBe(false)
  })
})

const applyMocks = vi.hoisted(() => ({
  findOrCreateCredlyIntegration: vi.fn(),
  upsertCredlyTemplateDecision: vi.fn(),
  deactivateCredlyTemplateMapping: vi.fn(),
  lockCredlyTemplateMapping: vi.fn(),
  onboardCredlyTemplate: vi.fn(),
  decommissionSyncUnitsForChannelIds: vi.fn(),
}))

vi.mock('@crowd/data-access-layer/src/credly', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@crowd/data-access-layer/src/credly')>()
  return {
    ...actual,
    findOrCreateCredlyIntegration: applyMocks.findOrCreateCredlyIntegration,
    upsertCredlyTemplateDecision: applyMocks.upsertCredlyTemplateDecision,
    deactivateCredlyTemplateMapping: applyMocks.deactivateCredlyTemplateMapping,
    lockCredlyTemplateMapping: applyMocks.lockCredlyTemplateMapping,
  }
})

vi.mock('@crowd/data-access-layer/src/connectors', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@crowd/data-access-layer/src/connectors')>()
  return {
    ...actual,
    decommissionSyncUnitsForChannelIds: applyMocks.decommissionSyncUnitsForChannelIds,
  }
})

vi.mock('./onboardCredlyTemplate', () => ({
  onboardCredlyTemplate: applyMocks.onboardCredlyTemplate,
}))

describe('applyDecisions', () => {
  const qx = { tx: (fn: (txQx: QueryExecutor) => unknown) => fn(qx) } as QueryExecutor

  beforeEach(() => {
    vi.clearAllMocks()
    applyMocks.findOrCreateCredlyIntegration.mockResolvedValue({
      id: 'int-k8s',
      created: false,
      restored: false,
    })
    applyMocks.upsertCredlyTemplateDecision.mockResolvedValue('inserted')
    applyMocks.onboardCredlyTemplate.mockResolvedValue({ outcome: 'created' })
    applyMocks.lockCredlyTemplateMapping.mockResolvedValue(true)
    applyMocks.decommissionSyncUnitsForChannelIds.mockResolvedValue(0)
  })

  it('onboards a sync unit for a mapped decision, seeding its backfill from the creation date', async () => {
    const result = await applyDecisions(
      qx,
      [decision('cka')],
      [],
      [
        {
          id: 'cka',
          name: 'CKA',
          createdAt: '2022-01-01T00:00:00.000Z',
          badgeCount: null,
          url: null,
        },
      ],
      [],
      { warn: vi.fn() } as never,
    )

    expect(applyMocks.findOrCreateCredlyIntegration).toHaveBeenCalledWith(qx, 'seg-k8s')
    expect(onboardCredlyTemplate).toHaveBeenCalledWith(qx, 'int-k8s', 'cka', 'cka', {
      since: expect.any(String),
      backfill: {
        floor: '2022-01-01T00:00:00.000Z',
        top: expect.any(String),
        since: null,
        until: null,
        page: 1,
      },
    })
    expect(result.unitsOnboarded).toBe(1)
    expect(result.outcomes.inserted).toBe(1)
    expect(applyMocks.lockCredlyTemplateMapping).toHaveBeenCalledWith(qx, 'cka', {
      segmentId: 'seg-k8s',
      integrationId: 'int-k8s',
    })
  })

  it('locks the mapping and counts a reactivated unit, for a template that reappeared', async () => {
    applyMocks.onboardCredlyTemplate.mockResolvedValueOnce({ outcome: 'reactivated' })

    const result = await applyDecisions(qx, [decision('cka')], [], [], [], {
      warn: vi.fn(),
    } as never)

    expect(result.unitsReactivated).toBe(1)
    expect(result.unitsOnboarded).toBe(0)
    expect(applyMocks.lockCredlyTemplateMapping).toHaveBeenCalledWith(qx, 'cka', {
      segmentId: 'seg-k8s',
      integrationId: 'int-k8s',
    })
  })

  it('does not lock the mapping when onboarding found the unit under another integration', async () => {
    applyMocks.onboardCredlyTemplate.mockResolvedValueOnce({
      outcome: 'skipped',
      reason: 'owned_by_other_integration',
      message: 'units exist for CKA under another integration',
    })

    await applyDecisions(qx, [decision('cka')], [], [], [], { warn: vi.fn() } as never)

    expect(applyMocks.lockCredlyTemplateMapping).not.toHaveBeenCalled()
  })

  it('reuses one integration per segment across several mapped templates', async () => {
    await applyDecisions(qx, [decision('cka'), decision('ckad')], [], [], [], {
      warn: vi.fn(),
    } as never)

    expect(applyMocks.findOrCreateCredlyIntegration).toHaveBeenCalledTimes(1)
    expect(applyMocks.onboardCredlyTemplate).toHaveBeenCalledTimes(2)
  })

  it('does not onboard a held or excluded decision', async () => {
    const result = await applyDecisions(
      qx,
      [decision('held', { status: 'held', holdReason: 'no_match', segmentId: null })],
      [],
      [],
      [],
      { warn: vi.fn() } as never,
    )

    expect(applyMocks.findOrCreateCredlyIntegration).not.toHaveBeenCalled()
    expect(applyMocks.onboardCredlyTemplate).not.toHaveBeenCalled()
    expect(result.unitsOnboarded).toBe(0)
  })

  it('skips onboarding when the decision is locked, since the stored target did not change', async () => {
    applyMocks.upsertCredlyTemplateDecision.mockResolvedValueOnce('locked')
    const log = { warn: vi.fn() }

    const result = await applyDecisions(qx, [decision('cka')], [], [], [], log as never)

    expect(applyMocks.onboardCredlyTemplate).not.toHaveBeenCalled()
    expect(result.outcomes.locked).toBe(1)
    expect(log.warn).toHaveBeenCalled()
  })

  it('deactivates a mapping no longer seen, regardless of its sync unit', async () => {
    applyMocks.deactivateCredlyTemplateMapping.mockResolvedValueOnce(1)

    const result = await applyDecisions(
      qx,
      [],
      [
        {
          kind: 'missing',
          badgeTemplateId: 'gone',
          badgeTemplateName: 'Gone',
          before: 'mapped:k8s',
          after: null,
        },
      ],
      [],
      [],
      { warn: vi.fn() } as never,
    )

    expect(applyMocks.deactivateCredlyTemplateMapping).toHaveBeenCalledWith(qx, 'gone')
    expect(result.deactivated).toBe(1)
    expect(applyMocks.decommissionSyncUnitsForChannelIds).not.toHaveBeenCalled()
  })

  it('decommissions the sync unit of a template a complete listing confirms is gone', async () => {
    applyMocks.deactivateCredlyTemplateMapping.mockResolvedValueOnce(1)
    applyMocks.decommissionSyncUnitsForChannelIds.mockResolvedValueOnce(1)

    const result = await applyDecisions(
      qx,
      [],
      [
        {
          kind: 'missing',
          badgeTemplateId: 'gone',
          badgeTemplateName: 'Gone',
          before: 'mapped:k8s',
          after: null,
        },
      ],
      [],
      [stored('gone', { integrationId: 'int-k8s' })],
      { warn: vi.fn() } as never,
    )

    expect(applyMocks.decommissionSyncUnitsForChannelIds).toHaveBeenCalledWith(qx, 'int-k8s', [
      'gone',
    ])
    expect(result.unitsDecommissioned).toBe(1)
  })

  it('leaves the unit running when another resolve relocked the mapping first', async () => {
    applyMocks.deactivateCredlyTemplateMapping.mockResolvedValueOnce(0)

    const result = await applyDecisions(
      qx,
      [],
      [
        {
          kind: 'missing',
          badgeTemplateId: 'gone',
          badgeTemplateName: 'Gone',
          before: 'mapped:k8s',
          after: null,
        },
      ],
      [],
      [stored('gone', { integrationId: 'int-k8s' })],
      { warn: vi.fn() } as never,
    )

    expect(applyMocks.decommissionSyncUnitsForChannelIds).not.toHaveBeenCalled()
    expect(result.deactivated).toBe(0)
    expect(result.unitsDecommissioned).toBe(0)
  })

  it('wraps the decision write, onboarding and mapping lock in one transaction', async () => {
    const txQx = {} as QueryExecutor
    const tx = vi.fn((fn: (txQx: QueryExecutor) => unknown) => fn(txQx))
    const txAwareQx = { tx } as unknown as QueryExecutor

    await applyDecisions(txAwareQx, [decision('cka')], [], [], [], { warn: vi.fn() } as never)

    expect(tx).toHaveBeenCalledTimes(1)
    expect(applyMocks.upsertCredlyTemplateDecision).toHaveBeenCalledWith(
      txQx,
      expect.objectContaining({ badgeTemplateId: 'cka' }),
    )
    expect(onboardCredlyTemplate).toHaveBeenCalledWith(
      txQx,
      'int-k8s',
      'cka',
      'cka',
      expect.any(Object),
    )
    expect(applyMocks.lockCredlyTemplateMapping).toHaveBeenCalledWith(txQx, 'cka', {
      segmentId: 'seg-k8s',
      integrationId: 'int-k8s',
    })
  })
})
