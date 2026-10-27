import { describe, expect, it } from 'vitest'

import { catalogueFixture } from './catalogue.fixture'
import type { CredlyTemplateOverride } from './overrides'
import { RULE_VERSION, resolveTemplates } from './resolver'
import type { ResolvedTemplate, TemplateToResolve } from './resolver'

const SEGMENTS = new Map([
  ['K8S', 'seg-k8s'],
  ['cncf', 'seg-cncf'],
  ['finops', 'seg-finops'],
  ['tlf', 'seg-tlf'],
  ['fabric', 'seg-fabric'],
  ['istio', 'seg-istio'],
  ['opencost', 'seg-opencost'],
  ['green-software', 'seg-green'],
])

function resolveOne(
  name: string,
  overrides: readonly CredlyTemplateOverride[] = [],
  extra: Partial<TemplateToResolve> = {},
): ResolvedTemplate {
  const [decision] = resolveTemplates({
    templates: [{ id: `tpl:${name}`, name, ...extra }],
    catalogue: catalogueFixture,
    segmentIdBySlug: SEGMENTS,
    overrides,
  })
  return decision
}

describe('resolveTemplates', () => {
  it.each([
    ['CKA: Certified Kubernetes Administrator', 'k8s', 'exact_sku', 'CKA'],
    ['CKAD: Certified Kubernetes Application Developer', 'k8s', 'exact_sku', 'CKAD'],
    ['KCNA: Kubernetes and Cloud Native Associate', 'k8s', 'exact_sku', 'KCNA'],
    ['CKS: Certified Kubernetes Security Specialist', 'k8s', 'exact_sku', 'CKS'],
    ['LFS158: Introduction to Kubernetes', 'k8s', 'exact_sku', 'LFS158'],
    ['LFC101: Inclusive Speaker Orientation', 'tlf', 'exact_sku', 'LFC101'],
    ['TSCB - Technical Services Certification B', 'tlf', 'exact_sku', 'TSCB'],
    ['Cloud Cost Monitoring for FinOps with OpenCost (LFS248)', 'opencost', 'exact_sku', 'LFS248'],
    ['CKA-BETA: Certified Kubernetes Administrator Beta', 'cncf', 'exact_sku', 'CKA-BETA'],
    ['CKA2: Certified Kubernetes Administrator', 'k8s', 'exact_sku', 'CKA2'],
  ])('maps %j through the exact SKU of the course group', (name, slug, rule, sku) => {
    expect(resolveOne(name)).toMatchObject({
      status: 'mapped',
      projectSlug: slug,
      segmentId: `seg-${slug}`,
      rule,
      matchedSku: sku,
      ruleVersion: RULE_VERSION,
    })
  })

  it('takes the course group project over the enrollment vote when both exist', () => {
    const decision = resolveOne('LFS158: Introduction to Kubernetes')
    expect(decision.rule).toBe('exact_sku')
  })

  it.each([
    ['LFS201: Essentials of Linux System Administration', 'tlf', 'LFS201'],
    ['LFS272: Hyperledger Fabric Administration', 'fabric', 'LFS272'],
  ])(
    'maps %j through the base SKU when every variant with a project agrees',
    (name, slug, base) => {
      expect(resolveOne(name)).toMatchObject({
        status: 'mapped',
        projectSlug: slug,
        rule: 'base_sku',
        matchedSku: base,
      })
    },
  )

  it('holds a base SKU whose variants point to different projects', () => {
    expect(resolveOne('KCNA-XYZ: Kubernetes and Cloud Native Associate Something')).toMatchObject({
      status: 'held',
      holdReason: 'ambiguous',
      evidence: { base: 'KCNA', projects: expect.arrayContaining(['k8s', 'cncf']) },
    })
  })

  it.each([
    ['FinOps Certified Practitioner', 'finops'],
    ['AI Value Trained: Level 1', 'finops'],
    ['FinOps Certified: AI Value', 'finops'],
  ])('maps %j by exact title when the name carries no code', (name, slug) => {
    expect(resolveOne(name)).toMatchObject({
      status: 'mapped',
      projectSlug: slug,
      rule: 'title',
      matchedSku: null,
    })
  })

  it('never sends FinOps templates to tokenomics through the FINOPS base code', () => {
    const decisions = resolveTemplates({
      templates: [
        { id: 'a', name: 'FinOps Certified Practitioner' },
        { id: 'b', name: 'AI Value Trained: Level 2' },
      ],
      catalogue: catalogueFixture,
      segmentIdBySlug: SEGMENTS,
      overrides: [],
    })
    expect(decisions.map((d) => d.projectSlug)).toEqual(['finops', 'finops'])
  })

  it('holds an archived course with no project in the catalogue as no_project', () => {
    expect(resolveOne('LFC131: Green Software for Practitioners')).toMatchObject({
      status: 'held',
      holdReason: 'no_project',
      projectSlug: null,
      evidence: { code: 'LFC131' },
    })
  })

  it('holds a mapped project that has no CDP segment yet and keeps the slug', () => {
    expect(resolveOne('LFS114: Introduction to free5GC')).toMatchObject({
      status: 'held',
      holdReason: 'no_segment',
      projectSlug: 'free5gc',
      segmentId: null,
      rule: 'exact_sku',
    })
  })

  it.each([
    'Exam Developer: Certified Kubernetes Administrator',
    'LF Live Mentorship Series: Mentor',
    'KubeCon + CloudNativeCon North America 2025 Speaker',
    'Zero to Merge (cohort 3)',
  ])('holds %j as no_match', (name) => {
    expect(resolveOne(name)).toMatchObject({ status: 'held', holdReason: 'no_match' })
  })

  it.each([
    'LFF101: ELearning Badge Test',
    'Do_Not_Use_Test - Program Committee: Zeph Dev Summit 2022',
  ])('holds %j as a test badge before any rule runs', (name) => {
    expect(resolveOne(name)).toMatchObject({ status: 'held', holdReason: 'test' })
  })

  it('sums enrollment rows per project before voting', () => {
    const catalogue = {
      ...catalogueFixture,
      courses: [
        ...catalogueFixture.courses,
        {
          sku: 'LFS9001',
          title: 'Split Course (LFS9001)',
          courseGroupId: null,
          isTestOrArchived: false,
        },
      ],
      enrollments: [
        ...catalogueFixture.enrollments,
        { courseCode: 'LFS9001', projectSlug: 'k8s', enrollments: 300 },
        { courseCode: 'LFS9001', projectSlug: 'k8s', enrollments: 300 },
        { courseCode: 'LFS9001', projectSlug: 'cncf', enrollments: 500 },
      ],
    }
    const [decision] = resolveTemplates({
      templates: [{ id: 'split', name: 'LFS9001: Split Course' }],
      catalogue,
      segmentIdBySlug: SEGMENTS,
      overrides: [],
    })
    expect(decision).toMatchObject({ status: 'mapped', projectSlug: 'k8s', rule: 'sku_vote' })
  })

  it('keeps a title with a descriptive parenthetical apart from the plain one', () => {
    const catalogue = {
      ...catalogueFixture,
      courseGroups: [
        ...catalogueFixture.courseGroups,
        { courseGroupId: 'g-plain', title: 'Kubernetes Networking', projectSlug: 'k8s' },
        { courseGroupId: 'g-adv', title: 'Kubernetes Networking (Advanced)', projectSlug: 'cncf' },
      ],
    }
    const [plain, advanced] = resolveTemplates({
      templates: [
        { id: 'plain', name: 'Kubernetes Networking' },
        { id: 'adv', name: 'Kubernetes Networking (Advanced)' },
      ],
      catalogue,
      segmentIdBySlug: SEGMENTS,
      overrides: [],
    })
    expect(plain).toMatchObject({ status: 'mapped', projectSlug: 'k8s', rule: 'title' })
    expect(advanced).toMatchObject({ status: 'mapped', projectSlug: 'cncf', rule: 'title' })
  })

  it('does not treat a course about testing as a test badge', () => {
    expect(resolveOne('LFC131: Test-Driven Green Software').holdReason).not.toBe('test')
  })

  it('resolves the segment slug case-insensitively', () => {
    expect(resolveOne('CKA: Certified Kubernetes Administrator').segmentId).toBe('seg-k8s')
  })

  it('carries the badge count and ignores a test catalogue row when a live one exists', () => {
    const decision = resolveOne('CKA: Certified Kubernetes Administrator', [], { badgeCount: 154 })
    expect(decision.badgeCount).toBe(154)
    expect(decision.projectSlug).toBe('k8s')
  })

  describe('overrides', () => {
    it('maps a held template to the reviewer project and records who decided', () => {
      const decision = resolveOne('LFC131: Green Software for Practitioners', [
        {
          badgeTemplateId: 'tpl:LFC131: Green Software for Practitioners',
          status: 'mapped',
          projectSlug: 'green-software',
          reviewedBy: 'joana',
          note: 'archived LF course, belongs to Green Software',
        },
      ])
      expect(decision).toMatchObject({
        status: 'mapped',
        projectSlug: 'green-software',
        segmentId: 'seg-green',
        rule: 'override',
        reviewedBy: 'joana',
      })
    })

    it('wins over a rule that would map the template elsewhere', () => {
      const decision = resolveOne('CKA: Certified Kubernetes Administrator', [
        {
          badgeTemplateId: 'tpl:CKA: Certified Kubernetes Administrator',
          status: 'mapped',
          projectSlug: 'cncf',
          reviewedBy: 'joana',
          note: 'roll up to CNCF for this test',
        },
      ])
      expect(decision).toMatchObject({ projectSlug: 'cncf', rule: 'override' })
    })

    it('excludes a template for good', () => {
      const decision = resolveOne('LFF101: ELearning Badge Test', [
        {
          badgeTemplateId: 'tpl:LFF101: ELearning Badge Test',
          status: 'excluded',
          reviewedBy: 'joana',
          note: 'test badge',
        },
      ])
      expect(decision).toMatchObject({
        status: 'excluded',
        rule: 'excluded',
        holdReason: null,
        reviewedBy: 'joana',
      })
    })

    it('holds an override whose project has no segment as no_segment', () => {
      const decision = resolveOne('PTCA: PyTorch Certified Associate', [
        {
          badgeTemplateId: 'tpl:PTCA: PyTorch Certified Associate',
          status: 'mapped',
          projectSlug: 'pytorch-foundation',
          reviewedBy: 'joana',
          note: 'foundation segment',
        },
      ])
      expect(decision).toMatchObject({
        status: 'held',
        holdReason: 'no_segment',
        projectSlug: 'pytorch-foundation',
        rule: 'override',
      })
    })
  })
})
