import { afterEach, test as base, beforeEach, describe, expect, vi } from 'vitest'

import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'
import {
  createActivityRelations,
  createMemberIdentities,
  createMemberOrganizations,
  createMemberSegmentAffiliations,
  createMembers,
  createOrganizationIdentities,
  createOrganizations,
  createSegments,
  generateActivityRelations,
  upsertMemberOrganizationAffiliationOverrides,
  withMemberDefaults,
  withMemberIdentityDefaults,
  withOrganizationDefaults,
  withSegmentDefaults,
} from '@crowd/test-kit/factories'
import {
  MemberIdentityType,
  OrganizationIdentityType,
  OrganizationSource,
  PlatformType,
} from '@crowd/types'

import { queryActivityRelations } from '../activityRelations'
import { deleteMemberSegmentAffiliations } from '../member_segment_affiliations'
import { deleteMemberOrganizations } from '../members/organizations'
import { insertOrganizationSegments } from '../organizations/segments'

import {
  prepareMemberOrganizationAffiliationTimeline,
  refreshMemberOrganizationAffiliations,
} from './index'
import type { TimelineItem } from './types'

const test = withQx(base)

const EPOCH = new Date('1970-01-01').toISOString()

/** Frozen "now" so prepare() dateEnd boundaries stay deterministic. */
const NOW = '2024-06-15T00:00:00.000Z'

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['Date'] })
  vi.setSystemTime(new Date(NOW))
})

afterEach(() => {
  vi.useRealTimers()
})

const SEGMENT_TREES = withSegmentDefaults([
  {
    name: 'Cloud Native Computing Foundation',
    slug: 'cncf',
    projects: [
      {
        name: 'Kubernetes',
        slug: 'kubernetes',
        subprojects: [{ name: 'kubernetes', slug: 'kubernetes' }],
      },
      {
        name: 'Harbor',
        slug: 'harbor',
        subprojects: [{ name: 'harbor', slug: 'harbor' }],
      },
    ],
  },
])

async function createLeafSegments(qx: QueryExecutor) {
  const { projectGroups } = await createSegments(qx, SEGMENT_TREES)
  const projects = projectGroups[0].projects
  return {
    kubernetes: projects[0].subprojects[0],
    harbor: projects[1].subprojects[0],
  }
}

async function createMemberWithIdentity(
  qx: QueryExecutor,
  identity: { platform?: PlatformType; type?: MemberIdentityType; value: string },
) {
  const [member] = await createMembers(qx, withMemberDefaults([{}]))
  const identities = await createMemberIdentities(
    qx,
    withMemberIdentityDefaults([
      {
        memberId: member.id,
        platform: identity.platform ?? PlatformType.GITHUB,
        type: identity.type ?? MemberIdentityType.USERNAME,
        value: identity.value,
        verified: true,
      },
    ]),
  )
  return { member, identities }
}

function baseItems(timeline: TimelineItem[]) {
  return timeline.filter((item) => !item.segmentId && !item.matchEmailDomain)
}

function manualItems(timeline: TimelineItem[]) {
  return timeline.filter((item) => !!item.segmentId)
}

function emailItems(timeline: TimelineItem[]) {
  return timeline.filter((item) => !!item.matchEmailDomain)
}

function expectTimeline(
  actual: TimelineItem[],
  expected: Array<{
    organizationId: string | null
    dateStart: string
    dateEnd: string | null
    segmentId?: string | null
    matchEmailDomain?: string | null
  }>,
) {
  expect(
    actual.map((item) => ({
      organizationId: item.organizationId,
      dateStart: item.dateStart,
      dateEnd: item.dateEnd,
      segmentId: item.segmentId ?? null,
      matchEmailDomain: item.matchEmailDomain ?? null,
    })),
  ).toEqual(
    expected.map((item) => ({
      segmentId: null,
      matchEmailDomain: null,
      ...item,
    })),
  )
}

async function countByOrg(
  qx: QueryExecutor,
  memberId: string,
  segmentId: string,
  organizationId: string | null,
): Promise<number> {
  const { count } = await queryActivityRelations(qx, {
    segmentIds: [segmentId],
    filter: {
      and: [{ memberId: { eq: memberId } }, { organizationId: { eq: organizationId } }],
    },
    countOnly: true,
  })
  return count
}

// Assert timeline shape here — do not re-prove affiliation decisions via activityRelations in refresh.
describe('prepareMemberOrganizationAffiliationTimeline', () => {
  test('no work history yields a null 1970 fallback', async ({ qx }) => {
    const [member] = await createMembers(qx, withMemberDefaults([{}]))

    const timeline = await prepareMemberOrganizationAffiliationTimeline(qx, member.id)

    expectTimeline(baseItems(timeline), [{ organizationId: null, dateStart: EPOCH, dateEnd: NOW }])
    expect(manualItems(timeline)).toHaveLength(0)
    expect(emailItems(timeline)).toHaveLength(0)
  })

  test('undated-only MO becomes the whole-timeline fallback', async ({ qx }) => {
    const [org] = await createOrganizations(
      qx,
      withOrganizationDefaults([{ displayName: 'Ant Group' }]),
    )
    const [member] = await createMembers(qx, withMemberDefaults([{}]))

    await createMemberOrganizations(qx, [
      {
        memberId: member.id,
        organizationId: org.id,
        dateStart: null,
        dateEnd: null,
        source: OrganizationSource.PROJECT_REGISTRY,
      },
    ])

    const timeline = await prepareMemberOrganizationAffiliationTimeline(qx, member.id)

    expectTimeline(baseItems(timeline), [
      { organizationId: org.id, dateStart: EPOCH, dateEnd: NOW },
    ])
  })

  test('undated MO covers pre-career when dated stints exist', async ({ qx }) => {
    const [employer, undated] = await createOrganizations(
      qx,
      withOrganizationDefaults([{ displayName: 'Wipro Limited' }, { displayName: 'Individual' }]),
    )
    const [member] = await createMembers(qx, withMemberDefaults([{}]))

    await createMemberOrganizations(qx, [
      {
        memberId: member.id,
        organizationId: employer.id,
        dateStart: '2017-07-01T00:00:00.000Z',
        dateEnd: '2020-01-01T00:00:00.000Z',
        title: 'Software Engineer',
        source: OrganizationSource.UI,
      },
      {
        memberId: member.id,
        organizationId: undated.id,
        dateStart: null,
        dateEnd: null,
        source: OrganizationSource.ENRICHMENT_CRUSTDATA,
      },
    ])

    const timeline = await prepareMemberOrganizationAffiliationTimeline(qx, member.id)

    expectTimeline(baseItems(timeline), [
      {
        organizationId: undated.id,
        dateStart: EPOCH,
        dateEnd: '2017-06-30T00:00:00.000Z',
      },
      {
        organizationId: employer.id,
        dateStart: '2017-07-01T00:00:00.000Z',
        dateEnd: '2020-01-01T00:00:00.000Z',
      },
      {
        organizationId: undated.id,
        dateStart: '2020-01-02T00:00:00.000Z',
        dateEnd: null,
      },
    ])
  })

  test('gap between jobs is filled by the undated fallback', async ({ qx }) => {
    const [early, later, undated] = await createOrganizations(
      qx,
      withOrganizationDefaults([
        { displayName: 'Wipro Limited' },
        { displayName: 'Qualcomm Inc' },
        { displayName: 'Individual - No Account' },
      ]),
    )
    const [member] = await createMembers(qx, withMemberDefaults([{}]))

    await createMemberOrganizations(qx, [
      {
        memberId: member.id,
        organizationId: early.id,
        dateStart: '2017-07-01T00:00:00.000Z',
        dateEnd: '2020-01-01T00:00:00.000Z',
        source: OrganizationSource.UI,
      },
      {
        memberId: member.id,
        organizationId: later.id,
        dateStart: '2020-03-01T00:00:00.000Z',
        dateEnd: '2021-08-01T00:00:00.000Z',
        source: OrganizationSource.UI,
      },
      {
        memberId: member.id,
        organizationId: undated.id,
        dateStart: null,
        dateEnd: null,
        source: OrganizationSource.ENRICHMENT_CRUSTDATA,
      },
    ])

    const timeline = await prepareMemberOrganizationAffiliationTimeline(qx, member.id)

    expectTimeline(baseItems(timeline), [
      {
        organizationId: undated.id,
        dateStart: EPOCH,
        dateEnd: '2017-06-30T00:00:00.000Z',
      },
      {
        organizationId: early.id,
        dateStart: '2017-07-01T00:00:00.000Z',
        dateEnd: '2020-01-01T00:00:00.000Z',
      },
      {
        organizationId: undated.id,
        dateStart: '2020-01-02T00:00:00.000Z',
        dateEnd: '2020-02-29T00:00:00.000Z',
      },
      {
        organizationId: later.id,
        dateStart: '2020-03-01T00:00:00.000Z',
        dateEnd: '2021-08-01T00:00:00.000Z',
      },
      {
        organizationId: undated.id,
        dateStart: '2021-08-02T00:00:00.000Z',
        dateEnd: null,
      },
    ])
  })

  test('allowAffiliation false drops overlapping stints from the timeline', async ({ qx }) => {
    const [employer, internship, foundation, undated] = await createOrganizations(
      qx,
      withOrganizationDefaults([
        { displayName: 'Capgemini' },
        { displayName: 'Outreachy' },
        { displayName: 'Cloud Native Computing Foundation (CNCF)' },
        { displayName: 'Individual' },
      ]),
    )
    const [member] = await createMembers(qx, withMemberDefaults([{}]))

    const mos = await createMemberOrganizations(qx, [
      {
        memberId: member.id,
        organizationId: employer.id,
        dateStart: '2021-08-01T00:00:00.000Z',
        dateEnd: null,
        title: 'Engineer',
        source: OrganizationSource.UI,
      },
      {
        memberId: member.id,
        organizationId: internship.id,
        dateStart: '2022-05-01T00:00:00.000Z',
        dateEnd: '2022-08-01T00:00:00.000Z',
        title: 'Intern',
        source: OrganizationSource.ENRICHMENT_CRUSTDATA,
      },
      {
        memberId: member.id,
        organizationId: foundation.id,
        dateStart: '2022-09-01T00:00:00.000Z',
        dateEnd: null,
        title: 'Release Shadow',
        source: OrganizationSource.ENRICHMENT_CRUSTDATA,
      },
      {
        memberId: member.id,
        organizationId: undated.id,
        dateStart: null,
        dateEnd: null,
        title: '',
        source: OrganizationSource.ENRICHMENT_CLEARBIT,
      },
    ])

    await upsertMemberOrganizationAffiliationOverrides(qx, [
      {
        memberId: member.id,
        memberOrganizationId: mos[1].id,
        allowAffiliation: false,
      },
      {
        memberId: member.id,
        memberOrganizationId: mos[2].id,
        allowAffiliation: false,
      },
    ])

    const timeline = await prepareMemberOrganizationAffiliationTimeline(qx, member.id)
    const base = baseItems(timeline)
    const orgIds = base.map((item) => item.organizationId)

    expect(orgIds[0]).toBe(undated.id)
    expect(orgIds).toContain(employer.id)
    expect(orgIds).not.toContain(internship.id)
    expect(orgIds).not.toContain(foundation.id)
    expectTimeline(
      base.filter((item) => item.organizationId === employer.id),
      [{ organizationId: employer.id, dateStart: '2021-08-01T00:00:00.000Z', dateEnd: null }],
    )
  })

  test('allowAffiliation false drops the MO from the timeline', async ({ qx }) => {
    const [org] = await createOrganizations(
      qx,
      withOrganizationDefaults([{ displayName: 'Blocked Co' }]),
    )
    const [member] = await createMembers(qx, withMemberDefaults([{}]))

    const [mo] = await createMemberOrganizations(qx, [
      {
        memberId: member.id,
        organizationId: org.id,
        dateStart: '2020-06-15T00:00:00.000Z',
        dateEnd: '2023-01-10T00:00:00.000Z',
        source: OrganizationSource.UI,
      },
    ])

    await upsertMemberOrganizationAffiliationOverrides(qx, [
      {
        memberId: member.id,
        memberOrganizationId: mo.id,
        allowAffiliation: false,
      },
    ])

    const timeline = await prepareMemberOrganizationAffiliationTimeline(qx, member.id)

    expectTimeline(baseItems(timeline), [{ organizationId: null, dateStart: EPOCH, dateEnd: NOW }])
  })

  test('primary undated MO wins over other undated as fallback', async ({ qx }) => {
    const [orgA, orgB] = await createOrganizations(
      qx,
      withOrganizationDefaults([{ displayName: 'NeoNephos' }, { displayName: 'SAP SE' }]),
    )
    const [member] = await createMembers(qx, withMemberDefaults([{}]))

    const [, moB] = await createMemberOrganizations(qx, [
      {
        memberId: member.id,
        organizationId: orgA.id,
        dateStart: null,
        dateEnd: null,
        source: OrganizationSource.PROJECT_REGISTRY,
      },
      {
        memberId: member.id,
        organizationId: orgB.id,
        dateStart: null,
        dateEnd: null,
        source: OrganizationSource.PROJECT_REGISTRY,
      },
    ])

    await upsertMemberOrganizationAffiliationOverrides(qx, [
      {
        memberId: member.id,
        memberOrganizationId: moB.id,
        isPrimaryWorkExperience: true,
      },
    ])

    const timeline = await prepareMemberOrganizationAffiliationTimeline(qx, member.id)

    expectTimeline(baseItems(timeline), [
      { organizationId: orgB.id, dateStart: EPOCH, dateEnd: NOW },
    ])
    expect(baseItems(timeline).every((item) => item.organizationId !== orgA.id)).toBe(true)
  })

  test('blacklisted titles like Investor, Mentor, and Board Member are excluded', async ({
    qx,
  }) => {
    const [investorOrg, mentorOrg, boardOrg, engOrg] = await createOrganizations(
      qx,
      withOrganizationDefaults([
        { displayName: 'Various Startups' },
        { displayName: 'Google' },
        { displayName: 'Acme Board' },
        { displayName: 'Red Hat' },
      ]),
    )
    const [member] = await createMembers(qx, withMemberDefaults([{}]))

    await createMemberOrganizations(qx, [
      {
        memberId: member.id,
        organizationId: investorOrg.id,
        dateStart: '2024-09-01T00:00:00.000Z',
        dateEnd: null,
        title: 'Investor',
        source: OrganizationSource.ENRICHMENT_CRUSTDATA,
      },
      {
        memberId: member.id,
        organizationId: mentorOrg.id,
        dateStart: '2016-06-01T00:00:00.000Z',
        dateEnd: '2022-05-01T00:00:00.000Z',
        title: 'Mentor',
        source: OrganizationSource.ENRICHMENT_CRUSTDATA,
      },
      {
        memberId: member.id,
        organizationId: boardOrg.id,
        dateStart: '2018-01-01T00:00:00.000Z',
        dateEnd: '2020-01-01T00:00:00.000Z',
        title: 'Board Member',
        source: OrganizationSource.ENRICHMENT_CRUSTDATA,
      },
      {
        memberId: member.id,
        organizationId: engOrg.id,
        dateStart: '2023-12-01T00:00:00.000Z',
        dateEnd: null,
        title: 'Principal Software Engineer',
        source: OrganizationSource.ENRICHMENT_CRUSTDATA,
      },
    ])

    const timeline = await prepareMemberOrganizationAffiliationTimeline(qx, member.id)
    const orgIds = baseItems(timeline).map((item) => item.organizationId)

    expect(orgIds).not.toContain(investorOrg.id)
    expect(orgIds).not.toContain(mentorOrg.id)
    expect(orgIds).not.toContain(boardOrg.id)
    expect(orgIds).toContain(engOrg.id)
  })

  test('soft-deleted MO is ignored', async ({ qx }) => {
    const [activeOrg, deletedOrg] = await createOrganizations(
      qx,
      withOrganizationDefaults([{ displayName: 'Red Hat' }, { displayName: 'Gone Co' }]),
    )
    const [member] = await createMembers(qx, withMemberDefaults([{}]))

    const [, deletedMo] = await createMemberOrganizations(qx, [
      {
        memberId: member.id,
        organizationId: activeOrg.id,
        dateStart: '2022-01-01T00:00:00.000Z',
        dateEnd: null,
        source: OrganizationSource.UI,
      },
      {
        memberId: member.id,
        organizationId: deletedOrg.id,
        dateStart: '2020-01-01T00:00:00.000Z',
        dateEnd: '2021-01-01T00:00:00.000Z',
        source: OrganizationSource.UI,
      },
    ])

    await deleteMemberOrganizations(qx, member.id, [deletedMo.id])

    const timeline = await prepareMemberOrganizationAffiliationTimeline(qx, member.id)
    const orgIds = baseItems(timeline).map((item) => item.organizationId)

    expect(orgIds).toContain(activeOrg.id)
    expect(orgIds).not.toContain(deletedOrg.id)
  })

  test('soft-deleted MSA is ignored', async ({ qx }) => {
    const [employer, sponsor] = await createOrganizations(
      qx,
      withOrganizationDefaults([{ displayName: 'Akuity Inc' }, { displayName: 'Red Hat' }]),
    )
    const [member] = await createMembers(qx, withMemberDefaults([{}]))
    const { kubernetes } = await createLeafSegments(qx)

    await createMemberOrganizations(qx, [
      {
        memberId: member.id,
        organizationId: employer.id,
        dateStart: '2021-08-01T00:00:00.000Z',
        dateEnd: null,
        source: OrganizationSource.UI,
      },
    ])

    const [msa] = await createMemberSegmentAffiliations(qx, [
      {
        memberId: member.id,
        segmentId: kubernetes.id,
        organizationId: sponsor.id,
        dateStart: null,
        dateEnd: null,
      },
    ])

    await deleteMemberSegmentAffiliations(qx, { ids: [msa.id] })

    const timeline = await prepareMemberOrganizationAffiliationTimeline(qx, member.id)

    expect(manualItems(timeline)).toHaveLength(0)
    expect(baseItems(timeline).every((item) => item.skipManualAffiliationSegments === false)).toBe(
      true,
    )
  })

  test('undated MSA is a 1970 catch-all; base timeline skips that segment', async ({ qx }) => {
    const [employer, sponsor] = await createOrganizations(
      qx,
      withOrganizationDefaults([{ displayName: 'Akuity Inc' }, { displayName: 'Red Hat' }]),
    )
    const [member] = await createMembers(qx, withMemberDefaults([{}]))
    const { kubernetes } = await createLeafSegments(qx)

    await createMemberOrganizations(qx, [
      {
        memberId: member.id,
        organizationId: employer.id,
        dateStart: '2021-08-01T00:00:00.000Z',
        dateEnd: null,
        source: OrganizationSource.UI,
      },
    ])

    await createMemberSegmentAffiliations(qx, [
      {
        memberId: member.id,
        segmentId: kubernetes.id,
        organizationId: sponsor.id,
        dateStart: null,
        dateEnd: null,
      },
    ])

    const timeline = await prepareMemberOrganizationAffiliationTimeline(qx, member.id)

    expectTimeline(manualItems(timeline), [
      {
        organizationId: sponsor.id,
        dateStart: EPOCH,
        dateEnd: null,
        segmentId: kubernetes.id,
      },
    ])
    expect(baseItems(timeline).length).toBeGreaterThan(0)
    expect(baseItems(timeline).every((item) => item.skipManualAffiliationSegments === true)).toBe(
      true,
    )
  })

  test('without MSA, base timeline does not skip manual affiliation segments', async ({ qx }) => {
    const [org] = await createOrganizations(qx, withOrganizationDefaults([{ displayName: 'Meta' }]))
    const [member] = await createMembers(qx, withMemberDefaults([{}]))

    await createMemberOrganizations(qx, [
      {
        memberId: member.id,
        organizationId: org.id,
        dateStart: '2019-06-01T00:00:00.000Z',
        dateEnd: null,
        source: OrganizationSource.UI,
      },
    ])

    const timeline = await prepareMemberOrganizationAffiliationTimeline(qx, member.id)

    expect(manualItems(timeline)).toHaveLength(0)
    expect(baseItems(timeline).every((item) => item.skipManualAffiliationSegments === false)).toBe(
      true,
    )
  })

  test('dated MSA keeps its date range on the segment timeline', async ({ qx }) => {
    const [org] = await createOrganizations(
      qx,
      withOrganizationDefaults([{ displayName: 'Akuity Inc' }]),
    )
    const [member] = await createMembers(qx, withMemberDefaults([{}]))
    const { kubernetes } = await createLeafSegments(qx)

    await createMemberSegmentAffiliations(qx, [
      {
        memberId: member.id,
        segmentId: kubernetes.id,
        organizationId: org.id,
        dateStart: '2021-08-01T00:00:00.000Z',
        dateEnd: null,
      },
    ])

    const timeline = await prepareMemberOrganizationAffiliationTimeline(qx, member.id)

    expectTimeline(manualItems(timeline), [
      {
        organizationId: org.id,
        dateStart: '2021-08-01T00:00:00.000Z',
        dateEnd: null,
        segmentId: kubernetes.id,
      },
    ])
  })

  test('multi-segment undated MSA pins different orgs per segment', async ({ qx }) => {
    const [sap, orlix] = await createOrganizations(
      qx,
      withOrganizationDefaults([{ displayName: 'SAP SE' }, { displayName: 'Orlix' }]),
    )
    const [member] = await createMembers(qx, withMemberDefaults([{}]))
    const { kubernetes, harbor } = await createLeafSegments(qx)

    await createMemberSegmentAffiliations(qx, [
      {
        memberId: member.id,
        segmentId: kubernetes.id,
        organizationId: sap.id,
        dateStart: null,
        dateEnd: null,
      },
      {
        memberId: member.id,
        segmentId: harbor.id,
        organizationId: orlix.id,
        dateStart: null,
        dateEnd: null,
      },
    ])

    const timeline = await prepareMemberOrganizationAffiliationTimeline(qx, member.id)
    const manual = manualItems(timeline)

    expect(manual).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          organizationId: sap.id,
          dateStart: EPOCH,
          segmentId: kubernetes.id,
        }),
        expect.objectContaining({
          organizationId: orlix.id,
          dateStart: EPOCH,
          segmentId: harbor.id,
        }),
      ]),
    )
    expect(manual).toHaveLength(2)
  })

  test('UI source beats enrichment when dated stints overlap without primary', async ({ qx }) => {
    const [uiOrg, enrichmentOrg] = await createOrganizations(
      qx,
      withOrganizationDefaults([{ displayName: 'SUSE' }, { displayName: 'Heptio' }]),
    )
    const [member] = await createMembers(qx, withMemberDefaults([{}]))

    await createMemberOrganizations(qx, [
      {
        memberId: member.id,
        organizationId: enrichmentOrg.id,
        dateStart: '2018-01-01T00:00:00.000Z',
        dateEnd: '2024-02-01T00:00:00.000Z',
        title: 'Community Manager',
        source: OrganizationSource.ENRICHMENT_CRUSTDATA,
      },
      {
        memberId: member.id,
        organizationId: uiOrg.id,
        dateStart: '2020-01-01T00:00:00.000Z',
        dateEnd: '2023-01-01T00:00:00.000Z',
        title: 'Advocate',
        source: OrganizationSource.UI,
      },
    ])

    const timeline = await prepareMemberOrganizationAffiliationTimeline(qx, member.id)

    expectTimeline(baseItems(timeline), [
      {
        organizationId: null,
        dateStart: EPOCH,
        dateEnd: '2017-12-31T00:00:00.000Z',
      },
      {
        organizationId: enrichmentOrg.id,
        dateStart: '2018-01-01T00:00:00.000Z',
        dateEnd: '2019-12-31T00:00:00.000Z',
      },
      {
        organizationId: uiOrg.id,
        dateStart: '2020-01-01T00:00:00.000Z',
        dateEnd: '2023-01-01T00:00:00.000Z',
      },
      {
        organizationId: enrichmentOrg.id,
        dateStart: '2023-01-02T00:00:00.000Z',
        dateEnd: '2024-02-01T00:00:00.000Z',
      },
      {
        organizationId: null,
        dateStart: '2024-02-02T00:00:00.000Z',
        dateEnd: NOW,
      },
    ])
  })

  test('longer stint wins when overlapping dated MOs share source and memberCount', async ({
    qx,
  }) => {
    const [longer, shorter] = await createOrganizations(
      qx,
      withOrganizationDefaults([{ displayName: 'Long Co' }, { displayName: 'Short Co' }]),
    )
    const [member] = await createMembers(qx, withMemberDefaults([{}]))

    await createMemberOrganizations(qx, [
      {
        memberId: member.id,
        organizationId: longer.id,
        dateStart: '2020-01-01T00:00:00.000Z',
        dateEnd: '2023-01-01T00:00:00.000Z',
        source: OrganizationSource.ENRICHMENT_CRUSTDATA,
      },
      {
        memberId: member.id,
        organizationId: shorter.id,
        dateStart: '2021-01-01T00:00:00.000Z',
        dateEnd: '2022-01-01T00:00:00.000Z',
        source: OrganizationSource.ENRICHMENT_CRUSTDATA,
      },
    ])

    const timeline = await prepareMemberOrganizationAffiliationTimeline(qx, member.id)

    expectTimeline(baseItems(timeline), [
      {
        organizationId: null,
        dateStart: EPOCH,
        dateEnd: '2019-12-31T00:00:00.000Z',
      },
      {
        organizationId: longer.id,
        dateStart: '2020-01-01T00:00:00.000Z',
        dateEnd: '2023-01-01T00:00:00.000Z',
      },
      {
        organizationId: null,
        dateStart: '2023-01-02T00:00:00.000Z',
        dateEnd: NOW,
      },
    ])
  })

  test('higher memberCount wins when overlapping dated MOs share source', async ({ qx }) => {
    const [popular, niche] = await createOrganizations(
      qx,
      withOrganizationDefaults([{ displayName: 'Popular Co' }, { displayName: 'Niche Co' }]),
    )
    const [member] = await createMembers(qx, withMemberDefaults([{}]))
    const { kubernetes } = await createLeafSegments(qx)

    // Niche stint is longer; popular must win via memberCount, not longest-range.
    await createMemberOrganizations(qx, [
      {
        memberId: member.id,
        organizationId: popular.id,
        dateStart: '2021-01-01T00:00:00.000Z',
        dateEnd: '2022-01-01T00:00:00.000Z',
        source: OrganizationSource.ENRICHMENT_CRUSTDATA,
      },
      {
        memberId: member.id,
        organizationId: niche.id,
        dateStart: '2020-01-01T00:00:00.000Z',
        dateEnd: '2023-01-01T00:00:00.000Z',
        source: OrganizationSource.ENRICHMENT_CRUSTDATA,
      },
    ])

    await insertOrganizationSegments(qx, [
      {
        organizationId: popular.id,
        segmentId: kubernetes.id,
        memberCount: 10_000,
        activityCount: 1,
        activeOn: ['github'],
        joinedAt: '2020-01-01T00:00:00.000Z',
        lastActive: '2024-01-01T00:00:00.000Z',
        avgContributorEngagement: 0,
      },
      {
        organizationId: niche.id,
        segmentId: kubernetes.id,
        memberCount: 10,
        activityCount: 1,
        activeOn: ['github'],
        joinedAt: '2020-01-01T00:00:00.000Z',
        lastActive: '2024-01-01T00:00:00.000Z',
        avgContributorEngagement: 0,
      },
    ])

    const timeline = await prepareMemberOrganizationAffiliationTimeline(qx, member.id)

    expectTimeline(baseItems(timeline), [
      {
        organizationId: null,
        dateStart: EPOCH,
        dateEnd: '2019-12-31T00:00:00.000Z',
      },
      {
        organizationId: niche.id,
        dateStart: '2020-01-01T00:00:00.000Z',
        dateEnd: '2020-12-31T00:00:00.000Z',
      },
      {
        organizationId: popular.id,
        dateStart: '2021-01-01T00:00:00.000Z',
        dateEnd: '2022-01-01T00:00:00.000Z',
      },
      {
        organizationId: niche.id,
        dateStart: '2022-01-02T00:00:00.000Z',
        dateEnd: '2023-01-01T00:00:00.000Z',
      },
      {
        organizationId: null,
        dateStart: '2023-01-02T00:00:00.000Z',
        dateEnd: NOW,
      },
    ])
  })

  test('company is preferred over university when dated stints overlap', async ({ qx }) => {
    const [company, university] = await createOrganizations(
      qx,
      withOrganizationDefaults([
        { displayName: 'Intel Corporation' },
        { displayName: 'New York University' },
      ]),
    )
    const [member] = await createMembers(qx, withMemberDefaults([{}]))

    await createOrganizationIdentities(qx, [
      {
        organizationId: company.id,
        platform: 'integration',
        value: 'intel.com',
        type: OrganizationIdentityType.PRIMARY_DOMAIN,
        verified: true,
      },
      {
        organizationId: university.id,
        platform: 'integration',
        value: 'nyu.edu',
        type: OrganizationIdentityType.PRIMARY_DOMAIN,
        verified: true,
      },
    ])

    // Same source; university stint is longer. Without company-over-uni, longest-range
    // (and equal source rank) would pick the university for the overlap.
    await createMemberOrganizations(qx, [
      {
        memberId: member.id,
        organizationId: university.id,
        dateStart: '2021-01-01T00:00:00.000Z',
        dateEnd: '2022-01-01T00:00:00.000Z',
        title: 'Student Researcher',
        source: OrganizationSource.ENRICHMENT_CRUSTDATA,
      },
      {
        memberId: member.id,
        organizationId: company.id,
        dateStart: '2021-06-01T00:00:00.000Z',
        dateEnd: '2021-09-01T00:00:00.000Z',
        title: 'Engineer',
        source: OrganizationSource.ENRICHMENT_CRUSTDATA,
      },
    ])

    const timeline = await prepareMemberOrganizationAffiliationTimeline(qx, member.id)

    expectTimeline(baseItems(timeline), [
      {
        organizationId: null,
        dateStart: EPOCH,
        dateEnd: '2020-12-31T00:00:00.000Z',
      },
      {
        organizationId: university.id,
        dateStart: '2021-01-01T00:00:00.000Z',
        dateEnd: '2021-05-31T00:00:00.000Z',
      },
      {
        organizationId: company.id,
        dateStart: '2021-06-01T00:00:00.000Z',
        dateEnd: '2021-09-01T00:00:00.000Z',
      },
      {
        organizationId: university.id,
        dateStart: '2021-09-02T00:00:00.000Z',
        dateEnd: '2022-01-01T00:00:00.000Z',
      },
      {
        organizationId: null,
        dateStart: '2022-01-02T00:00:00.000Z',
        dateEnd: NOW,
      },
    ])
  })

  test('verified primary-domain yields an email timeline item', async ({ qx }) => {
    const [org] = await createOrganizations(
      qx,
      withOrganizationDefaults([{ displayName: 'Red Hat' }]),
    )
    const [member] = await createMembers(qx, withMemberDefaults([{}]))

    await createOrganizationIdentities(qx, [
      {
        organizationId: org.id,
        platform: 'integration',
        value: 'redhat.com',
        type: OrganizationIdentityType.PRIMARY_DOMAIN,
        verified: true,
      },
    ])

    await createMemberOrganizations(qx, [
      {
        memberId: member.id,
        organizationId: org.id,
        dateStart: '2023-12-01T00:00:00.000Z',
        dateEnd: null,
        source: OrganizationSource.UI,
      },
    ])

    const timeline = await prepareMemberOrganizationAffiliationTimeline(qx, member.id)

    expectTimeline(emailItems(timeline), [
      {
        organizationId: org.id,
        dateStart: EPOCH,
        dateEnd: null,
        matchEmailDomain: 'redhat.com',
      },
    ])
    expect(
      baseItems(timeline).every((item) => item.excludeEmailDomains?.includes('redhat.com')),
    ).toBe(true)
  })
})

// Write path only — affiliation decisions are covered in prepare above.
describe('refreshMemberOrganizationAffiliations', () => {
  test('writes exact orgIds for fixed timestamps across career, gap, and undated', async ({
    qx,
  }) => {
    const [early, later, undated] = await createOrganizations(
      qx,
      withOrganizationDefaults([
        { displayName: 'Wipro Limited' },
        { displayName: 'Qualcomm Inc' },
        { displayName: 'Individual' },
      ]),
    )
    const { member, identities } = await createMemberWithIdentity(qx, { value: 'contributor' })
    const { kubernetes } = await createLeafSegments(qx)

    await createMemberOrganizations(qx, [
      {
        memberId: member.id,
        organizationId: early.id,
        dateStart: '2017-07-01T00:00:00.000Z',
        dateEnd: '2020-01-01T00:00:00.000Z',
        source: OrganizationSource.UI,
      },
      {
        memberId: member.id,
        organizationId: later.id,
        dateStart: '2020-03-01T00:00:00.000Z',
        dateEnd: '2021-08-01T00:00:00.000Z',
        source: OrganizationSource.UI,
      },
      {
        memberId: member.id,
        organizationId: undated.id,
        dateStart: null,
        dateEnd: null,
        source: OrganizationSource.ENRICHMENT_CRUSTDATA,
      },
    ])

    const seeded = await createActivityRelations(qx, [
      ...generateActivityRelations({
        memberId: member.id,
        segmentId: kubernetes.id,
        identities,
        count: 50,
        timestamp: { from: '2018-04-01T00:00:00.000Z', to: '2019-06-01T00:00:00.000Z' },
      }),
      ...generateActivityRelations({
        memberId: member.id,
        segmentId: kubernetes.id,
        identities,
        count: 40,
        timestamp: { from: '2020-04-01T00:00:00.000Z', to: '2021-01-01T00:00:00.000Z' },
      }),
      ...generateActivityRelations({
        memberId: member.id,
        segmentId: kubernetes.id,
        identities,
        count: 30,
        timestamp: { from: '2020-01-15T00:00:00.000Z', to: '2020-02-15T00:00:00.000Z' },
      }),
      ...generateActivityRelations({
        memberId: member.id,
        segmentId: kubernetes.id,
        identities,
        count: 20,
        timestamp: { from: '2015-01-01T00:00:00.000Z', to: '2016-01-01T00:00:00.000Z' },
      }),
    ])

    expect(seeded).toHaveLength(140)

    await refreshMemberOrganizationAffiliations(qx, member.id)

    expect(await countByOrg(qx, member.id, kubernetes.id, early.id)).toBe(50)
    expect(await countByOrg(qx, member.id, kubernetes.id, later.id)).toBe(40)
    expect(await countByOrg(qx, member.id, kubernetes.id, undated.id)).toBe(50) // gap 30 + pre-career 20
  })

  test('applies MSA organization to activity relations on that segment over MO', async ({ qx }) => {
    const [employer, sponsor] = await createOrganizations(
      qx,
      withOrganizationDefaults([{ displayName: 'Akuity Inc' }, { displayName: 'Red Hat' }]),
    )
    const { member, identities } = await createMemberWithIdentity(qx, { value: 'maintainer' })
    const { kubernetes } = await createLeafSegments(qx)

    await createMemberOrganizations(qx, [
      {
        memberId: member.id,
        organizationId: employer.id,
        dateStart: '2020-01-01T00:00:00.000Z',
        dateEnd: null,
        source: OrganizationSource.UI,
      },
    ])

    await createMemberSegmentAffiliations(qx, [
      {
        memberId: member.id,
        segmentId: kubernetes.id,
        organizationId: sponsor.id,
        dateStart: null,
        dateEnd: null,
      },
    ])

    const seeded = await createActivityRelations(
      qx,
      generateActivityRelations({
        memberId: member.id,
        segmentId: kubernetes.id,
        identities,
        count: 100,
        timestamp: { from: '2021-01-01T00:00:00.000Z', to: '2022-01-01T00:00:00.000Z' },
      }),
    )

    await refreshMemberOrganizationAffiliations(qx, member.id)

    expect(await countByOrg(qx, member.id, kubernetes.id, sponsor.id)).toBe(seeded.length)
    expect(await countByOrg(qx, member.id, kubernetes.id, employer.id)).toBe(0)
  })

  test('dated MSA applies only inside its window; MO covers activities outside', async ({ qx }) => {
    const [employer, sponsor] = await createOrganizations(
      qx,
      withOrganizationDefaults([{ displayName: 'Akuity Inc' }, { displayName: 'Red Hat' }]),
    )
    const { member, identities } = await createMemberWithIdentity(qx, { value: 'contributor' })
    const { kubernetes } = await createLeafSegments(qx)

    await createMemberOrganizations(qx, [
      {
        memberId: member.id,
        organizationId: employer.id,
        dateStart: '2020-01-01T00:00:00.000Z',
        dateEnd: null,
        source: OrganizationSource.UI,
      },
    ])

    await createMemberSegmentAffiliations(qx, [
      {
        memberId: member.id,
        segmentId: kubernetes.id,
        organizationId: sponsor.id,
        dateStart: '2021-01-01T00:00:00.000Z',
        dateEnd: '2021-06-01T00:00:00.000Z',
      },
    ])

    await createActivityRelations(qx, [
      ...generateActivityRelations({
        memberId: member.id,
        segmentId: kubernetes.id,
        identities,
        count: 40,
        timestamp: { from: '2021-02-01T00:00:00.000Z', to: '2021-05-01T00:00:00.000Z' },
      }),
      ...generateActivityRelations({
        memberId: member.id,
        segmentId: kubernetes.id,
        identities,
        count: 30,
        timestamp: { from: '2022-01-01T00:00:00.000Z', to: '2022-06-01T00:00:00.000Z' },
      }),
    ])

    await refreshMemberOrganizationAffiliations(qx, member.id)

    expect(await countByOrg(qx, member.id, kubernetes.id, sponsor.id)).toBe(40)
    expect(await countByOrg(qx, member.id, kubernetes.id, employer.id)).toBe(30)
  })

  test('email-domain activities route to matching org over the date timeline', async ({ qx }) => {
    const [emailOrg, datedOrg] = await createOrganizations(
      qx,
      withOrganizationDefaults([{ displayName: 'Red Hat' }, { displayName: 'Previous Employer' }]),
    )
    const { member, identities } = await createMemberWithIdentity(qx, {
      platform: PlatformType.GITHUB,
      type: MemberIdentityType.EMAIL,
      value: 'dev@redhat.com',
    })
    const { kubernetes } = await createLeafSegments(qx)

    await createOrganizationIdentities(qx, [
      {
        organizationId: emailOrg.id,
        platform: 'integration',
        value: 'redhat.com',
        type: OrganizationIdentityType.PRIMARY_DOMAIN,
        verified: true,
      },
    ])

    await createMemberOrganizations(qx, [
      {
        memberId: member.id,
        organizationId: datedOrg.id,
        dateStart: '2020-01-01T00:00:00.000Z',
        dateEnd: null,
        source: OrganizationSource.UI,
      },
      {
        memberId: member.id,
        organizationId: emailOrg.id,
        dateStart: '2023-12-01T00:00:00.000Z',
        dateEnd: null,
        source: OrganizationSource.EMAIL_DOMAIN,
      },
    ])

    const seeded = await createActivityRelations(
      qx,
      generateActivityRelations({
        memberId: member.id,
        segmentId: kubernetes.id,
        identities,
        count: 25,
        // Inside datedOrg's window — email domain should still win over the date timeline.
        timestamp: { from: '2021-01-01T00:00:00.000Z', to: '2021-06-01T00:00:00.000Z' },
      }),
    )

    await refreshMemberOrganizationAffiliations(qx, member.id)

    expect(await countByOrg(qx, member.id, kubernetes.id, emailOrg.id)).toBe(seeded.length)
    expect(await countByOrg(qx, member.id, kubernetes.id, datedOrg.id)).toBe(0)
  })

  test('during company/university overlap, non-email follows company while email domains keep their orgs', async ({
    qx,
  }) => {
    const [company, university] = await createOrganizations(
      qx,
      withOrganizationDefaults([
        { displayName: 'Intel Corporation' },
        { displayName: 'New York University' },
      ]),
    )
    const [member] = await createMembers(qx, withMemberDefaults([{}]))
    const { kubernetes } = await createLeafSegments(qx)

    const [githubIdentity, uniEmailIdentity, companyEmailIdentity] = await createMemberIdentities(
      qx,
      withMemberIdentityDefaults([
        {
          memberId: member.id,
          platform: PlatformType.GITHUB,
          type: MemberIdentityType.USERNAME,
          value: 'student-dev',
          verified: true,
        },
        {
          memberId: member.id,
          platform: PlatformType.GITHUB,
          type: MemberIdentityType.EMAIL,
          value: 'student@nyu.edu',
          verified: true,
        },
        {
          memberId: member.id,
          platform: PlatformType.GITHUB,
          type: MemberIdentityType.EMAIL,
          value: 'eng@intel.com',
          verified: true,
        },
      ]),
    )

    await createOrganizationIdentities(qx, [
      {
        organizationId: company.id,
        platform: 'integration',
        value: 'intel.com',
        type: OrganizationIdentityType.PRIMARY_DOMAIN,
        verified: true,
      },
      {
        organizationId: university.id,
        platform: 'integration',
        value: 'nyu.edu',
        type: OrganizationIdentityType.PRIMARY_DOMAIN,
        verified: true,
      },
    ])

    await createMemberOrganizations(qx, [
      {
        memberId: member.id,
        organizationId: university.id,
        dateStart: '2021-01-01T00:00:00.000Z',
        dateEnd: '2022-01-01T00:00:00.000Z',
        title: 'Student Researcher',
        source: OrganizationSource.ENRICHMENT_CRUSTDATA,
      },
      {
        memberId: member.id,
        organizationId: company.id,
        dateStart: '2021-06-01T00:00:00.000Z',
        dateEnd: '2021-09-01T00:00:00.000Z',
        title: 'Engineer',
        source: OrganizationSource.ENRICHMENT_CRUSTDATA,
      },
    ])

    const overlap = {
      from: '2021-07-01T00:00:00.000Z',
      to: '2021-07-15T00:00:00.000Z',
    } as const

    await createActivityRelations(qx, [
      ...generateActivityRelations({
        memberId: member.id,
        segmentId: kubernetes.id,
        identities: [githubIdentity],
        count: 10,
        timestamp: overlap,
      }),
      ...generateActivityRelations({
        memberId: member.id,
        segmentId: kubernetes.id,
        identities: [uniEmailIdentity],
        count: 8,
        timestamp: overlap,
      }),
      ...generateActivityRelations({
        memberId: member.id,
        segmentId: kubernetes.id,
        identities: [companyEmailIdentity],
        count: 6,
        timestamp: overlap,
      }),
    ])

    await refreshMemberOrganizationAffiliations(qx, member.id)

    expect(await countByOrg(qx, member.id, kubernetes.id, company.id)).toBe(16) // 10 GH + 6 @intel.com
    expect(await countByOrg(qx, member.id, kubernetes.id, university.id)).toBe(8) // @nyu.edu
  })

  test('clears stale organizationId when timeline resolves to null', async ({ qx }) => {
    const [staleOrg] = await createOrganizations(
      qx,
      withOrganizationDefaults([{ displayName: 'Stale Co' }]),
    )
    const { member, identities } = await createMemberWithIdentity(qx, { value: 'no-employer' })
    const { kubernetes } = await createLeafSegments(qx)

    // No member organizations → prepare yields a null fallback for the whole timeline.
    const seeded = await createActivityRelations(
      qx,
      generateActivityRelations({
        memberId: member.id,
        segmentId: kubernetes.id,
        identities,
        count: 20,
        timestamp: { from: '2021-01-01T00:00:00.000Z', to: '2021-06-01T00:00:00.000Z' },
      }).map((row) => ({ ...row, organizationId: staleOrg.id })),
    )

    expect(await countByOrg(qx, member.id, kubernetes.id, staleOrg.id)).toBe(seeded.length)

    await refreshMemberOrganizationAffiliations(qx, member.id)

    expect(await countByOrg(qx, member.id, kubernetes.id, staleOrg.id)).toBe(0)
    expect(await countByOrg(qx, member.id, kubernetes.id, null)).toBe(seeded.length)
  })
})
