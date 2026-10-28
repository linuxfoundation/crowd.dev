import { describe, expect, it, vi } from 'vitest'

import { fetchBadgeTemplatesPage } from '@crowd/connectors/src/connectors/credly/apiClient'
import type { ResolvedTemplate } from '@crowd/connectors/src/connectors/credly/mapping/resolver'
import type { ICredlyStoredTemplateMapping } from '@crowd/data-access-layer/src/credly'

import {
  buildPlan,
  loadCatalogue,
  loadCredlyTemplates,
  loadSubprojectSegments,
} from './credlyTemplateResolve'

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
    data: { id: string; name: string; badges_count?: unknown }[],
    total_count: number,
  ) =>
    ({
      data,
      metadata: { count: data.length, current_page: 1, total_count, total_pages: 2 },
    }) as never

  it('reads every page and reports the listing complete when totals and ids line up', async () => {
    vi.mocked(fetchBadgeTemplatesPage)
      .mockResolvedValueOnce(page([{ id: 'a', name: 'A', badges_count: 3 }], 2))
      .mockResolvedValueOnce(page([{ id: 'b', name: 'B', badges_count: null }], 2))

    const listing = await loadCredlyTemplates(http, 'org', log)

    expect(listing.complete).toBe(true)
    expect(listing.templates).toEqual([
      { id: 'a', name: 'A', badgeCount: 3, url: null },
      { id: 'b', name: 'B', badgeCount: null, url: null },
    ])
  })

  it('reports an incomplete listing when a template shows up twice across pages', async () => {
    vi.mocked(fetchBadgeTemplatesPage)
      .mockResolvedValueOnce(page([{ id: 'a', name: 'A' }], 2))
      .mockResolvedValueOnce(page([{ id: 'a', name: 'A' }], 2))

    expect((await loadCredlyTemplates(http, 'org', log)).complete).toBe(false)
  })
})
