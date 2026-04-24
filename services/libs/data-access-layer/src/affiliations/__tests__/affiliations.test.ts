import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { QueryExecutor } from '../../queryExecutor'
import {
  buildTimeline,
  resolveAffiliationsByMemberIds,
  selectPrimaryWorkExperience,
} from '../index'
import type { IWorkExperienceResolution } from '../index'

// Mocks are hoisted before imports — intercept transitive dependencies that
// require a live database or external services.
vi.mock('@crowd/database', () => ({}))
vi.mock('@crowd/logging', () => ({
  getServiceLogger: () => ({ info: vi.fn(), debug: vi.fn(), warn: vi.fn(), error: vi.fn() }),
  getServiceChildLogger: () => ({
    info: vi.fn(),
    debug: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
  }),
  LoggerBase: class {},
}))
vi.mock('@crowd/redis', () => ({}))

// Provide the blacklist constant used by findWorkExperiencesBulk without loading
// the full members/base module (which has heavy DB dependencies).
vi.mock('../../members/base', () => ({
  BLACKLISTED_MEMBER_TITLES: ['investor', 'mentor', 'board member'],
}))

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Mirror of the private startOfDay() in affiliations/index.ts.
// Kept in sync manually: sets a date to UTC midnight so boundary comparisons
// in tests produce the same values as the production code.
function startOfDay(date: string): Date {
  const d = new Date(date)
  d.setUTCHours(0, 0, 0, 0)
  return d
}

function makeRow(overrides: Partial<IWorkExperienceResolution> = {}): IWorkExperienceResolution {
  return {
    id: 'row-1',
    memberId: 'member-1',
    organizationId: 'org-1',
    organizationName: 'Test Org',
    title: null,
    dateStart: null,
    dateEnd: null,
    createdAt: '2020-01-01T00:00:00.000Z',
    isPrimaryWorkExperience: false,
    memberCount: 1,
    segmentId: null,
    ...overrides,
  }
}

// ---------------------------------------------------------------------------
// selectPrimaryWorkExperience
// ---------------------------------------------------------------------------

describe('selectPrimaryWorkExperience', () => {
  it('returns the single org when the array has exactly one element', () => {
    const org = makeRow({ organizationName: 'Solo' })
    expect(selectPrimaryWorkExperience([org])).toBe(org)
  })

  it('picks the manual affiliation (segmentId != null) over work experiences', () => {
    const workExp = makeRow({
      organizationId: 'work',
      organizationName: 'Work Inc',
      segmentId: null,
    })
    const manual = makeRow({
      organizationId: 'manual',
      organizationName: 'Manual Org',
      segmentId: 'seg-1',
    })
    const result = selectPrimaryWorkExperience([workExp, manual])
    expect(result.organizationName).toBe('Manual Org')
  })

  it('picks the manual with the longest date range when there are multiple manual affiliations', () => {
    const shortManual = makeRow({
      organizationId: 'm1',
      organizationName: 'Short',
      segmentId: 'seg-1',
      dateStart: '2020-01-01',
      dateEnd: '2020-06-30',
    })
    const longManual = makeRow({
      organizationId: 'm2',
      organizationName: 'Long',
      segmentId: 'seg-2',
      dateStart: '2018-01-01',
      dateEnd: '2022-12-31',
    })
    const result = selectPrimaryWorkExperience([shortManual, longManual])
    expect(result.organizationName).toBe('Long')
  })

  it('picks the isPrimaryWorkExperience=true row when no manual affiliations exist', () => {
    const plain = makeRow({
      organizationId: 'plain',
      organizationName: 'Plain',
      dateStart: '2018-01-01',
    })
    const primary = makeRow({
      organizationId: 'primary',
      organizationName: 'Primary',
      isPrimaryWorkExperience: true,
      dateStart: '2020-01-01',
    })
    const result = selectPrimaryWorkExperience([plain, primary])
    expect(result.organizationName).toBe('Primary')
  })

  it('prefers the primary row with a dateStart over one without', () => {
    const noDate = makeRow({
      organizationId: 'nodates',
      organizationName: 'NoDates',
      isPrimaryWorkExperience: true,
      dateStart: null,
    })
    const withDate = makeRow({
      organizationId: 'wdates',
      organizationName: 'WithDates',
      isPrimaryWorkExperience: true,
      dateStart: '2020-01-01',
    })
    const result = selectPrimaryWorkExperience([noDate, withDate])
    expect(result.organizationName).toBe('WithDates')
  })

  it('picks the single dated org when no primary and only one has a dateStart', () => {
    const undated = makeRow({ organizationId: 'u', organizationName: 'Undated', dateStart: null })
    const dated = makeRow({
      organizationId: 'd',
      organizationName: 'Dated',
      dateStart: '2020-01-01',
    })
    const result = selectPrimaryWorkExperience([undated, dated])
    expect(result.organizationName).toBe('Dated')
  })

  it('picks the org with more members when no primary and multiple have dates', () => {
    const small = makeRow({
      organizationId: 'small',
      organizationName: 'Small',
      dateStart: '2020-01-01',
      memberCount: 10,
    })
    const large = makeRow({
      organizationId: 'large',
      organizationName: 'Large',
      dateStart: '2019-01-01',
      memberCount: 100,
    })
    const result = selectPrimaryWorkExperience([small, large])
    expect(result.organizationName).toBe('Large')
  })

  it('falls through to longest date range when memberCounts are tied', () => {
    const shortRange = makeRow({
      organizationId: 'short',
      organizationName: 'ShortRange',
      dateStart: '2020-01-01',
      dateEnd: '2021-12-31',
      memberCount: 10,
    })
    const longRange = makeRow({
      organizationId: 'long',
      organizationName: 'LongRange',
      dateStart: '2018-01-01',
      dateEnd: '2022-12-31',
      memberCount: 10,
    })
    const result = selectPrimaryWorkExperience([shortRange, longRange])
    expect(result.organizationName).toBe('LongRange')
  })

  it('email-domain beats enrichment when both are dated', () => {
    const enrichment = makeRow({
      organizationId: 'enrichment',
      organizationName: 'Enrichment Org',
      dateStart: '2020-01-01',
      source: 'enrichment-progai',
    })
    const emailDomain = makeRow({
      organizationId: 'email',
      organizationName: 'Email Org',
      dateStart: '2020-01-01',
      source: 'email-domain',
    })
    expect(selectPrimaryWorkExperience([enrichment, emailDomain]).organizationName).toBe(
      'Email Org',
    )
  })

  it('ui beats email-domain when both are dated', () => {
    const emailDomain = makeRow({
      organizationId: 'email',
      organizationName: 'Email Org',
      dateStart: '2020-01-01',
      source: 'email-domain',
    })
    const ui = makeRow({
      organizationId: 'ui',
      organizationName: 'UI Org',
      dateStart: '2020-01-01',
      source: 'ui',
    })
    expect(selectPrimaryWorkExperience([emailDomain, ui]).organizationName).toBe('UI Org')
  })

  it('ui beats enrichment when both are dated', () => {
    const enrichment = makeRow({
      organizationId: 'enrichment',
      organizationName: 'Enrichment Org',
      dateStart: '2020-01-01',
      source: 'enrichment-clearbit',
    })
    const ui = makeRow({
      organizationId: 'ui',
      organizationName: 'UI Org',
      dateStart: '2020-01-01',
      source: 'ui',
    })
    expect(selectPrimaryWorkExperience([enrichment, ui]).organizationName).toBe('UI Org')
  })

  it('falls through to member count when source tiers are equal', () => {
    const small = makeRow({
      organizationId: 'small',
      organizationName: 'Small Enrichment',
      dateStart: '2020-01-01',
      memberCount: 10,
      source: 'enrichment-progai',
    })
    const large = makeRow({
      organizationId: 'large',
      organizationName: 'Large Enrichment',
      dateStart: '2020-01-01',
      memberCount: 100,
      source: 'enrichment-progai',
    })
    expect(selectPrimaryWorkExperience([small, large]).organizationName).toBe('Large Enrichment')
  })

  it('undated rows are not affected by source priority — dated enrichment beats undated email-domain', () => {
    const undatedEmailDomain = makeRow({
      organizationId: 'email',
      organizationName: 'Email Org',
      dateStart: null,
      source: 'email-domain',
    })
    const datedEnrichment = makeRow({
      organizationId: 'enrichment',
      organizationName: 'Enrichment Org',
      dateStart: '2020-01-01',
      source: 'enrichment-progai',
    })
    expect(
      selectPrimaryWorkExperience([undatedEmailDomain, datedEnrichment]).organizationName,
    ).toBe('Enrichment Org')
  })
})

// ---------------------------------------------------------------------------
// buildTimeline
// ---------------------------------------------------------------------------

describe('buildTimeline', () => {
  it('produces a single ongoing period for one undated-end org', () => {
    const org = makeRow({ organizationName: 'Acme', dateStart: '2020-01-01', dateEnd: null })
    const boundaries = [startOfDay('2020-01-01'), startOfDay('2026-01-01')]

    const result = buildTimeline([org], null, boundaries)

    expect(result).toEqual([
      {
        organization: 'Acme',
        startDate: startOfDay('2020-01-01').toISOString(),
        endDate: null,
      },
    ])
  })

  it('produces a single closed period for an org with both start and end dates', () => {
    const org = makeRow({
      organizationName: 'Acme',
      dateStart: '2020-01-01',
      dateEnd: '2022-12-31',
    })
    // afterEnd boundary: day after dateEnd
    const boundaries = [startOfDay('2020-01-01'), startOfDay('2023-01-01')]

    const result = buildTimeline([org], null, boundaries)

    expect(result).toEqual([
      {
        organization: 'Acme',
        startDate: startOfDay('2020-01-01').toISOString(),
        endDate: startOfDay('2022-12-31').toISOString(),
      },
    ])
  })

  it('produces two sequential periods when orgs share a boundary with no gap', () => {
    const org1 = makeRow({
      organizationId: 'o1',
      organizationName: 'Org1',
      dateStart: '2018-01-01',
      dateEnd: '2020-12-31',
    })
    const org2 = makeRow({
      organizationId: 'o2',
      organizationName: 'Org2',
      dateStart: '2021-01-01',
      dateEnd: null,
    })
    // 2021-01-01 is both the afterEnd of org1 and the dateStart of org2
    const boundaries = [
      startOfDay('2018-01-01'),
      startOfDay('2021-01-01'),
      startOfDay('2026-01-01'),
    ]

    const result = buildTimeline([org1, org2], null, boundaries)

    expect(result).toEqual([
      {
        organization: 'Org1',
        startDate: startOfDay('2018-01-01').toISOString(),
        endDate: startOfDay('2020-12-31').toISOString(),
      },
      {
        organization: 'Org2',
        startDate: startOfDay('2021-01-01').toISOString(),
        endDate: null,
      },
    ])
  })

  it('fills a gap between two orgs with the fallback org', () => {
    const org1 = makeRow({
      organizationId: 'o1',
      organizationName: 'Org1',
      dateStart: '2018-01-01',
      dateEnd: '2019-12-31',
    })
    const org2 = makeRow({
      organizationId: 'o2',
      organizationName: 'Org2',
      dateStart: '2021-01-01',
      dateEnd: null,
    })
    const fallback = makeRow({ organizationId: 'fb', organizationName: 'Fallback' })
    const boundaries = [
      startOfDay('2018-01-01'),
      startOfDay('2020-01-01'),
      startOfDay('2021-01-01'),
      startOfDay('2026-01-01'),
    ]

    const result = buildTimeline([org1, org2], fallback, boundaries)

    expect(result).toEqual([
      {
        organization: 'Org1',
        startDate: startOfDay('2018-01-01').toISOString(),
        endDate: startOfDay('2019-12-31').toISOString(),
      },
      {
        organization: 'Fallback',
        startDate: startOfDay('2020-01-01').toISOString(),
        endDate: startOfDay('2020-12-31').toISOString(),
      },
      {
        organization: 'Org2',
        startDate: startOfDay('2021-01-01').toISOString(),
        endDate: null,
      },
    ])
  })

  it('leaves a gap unfilled when there is no fallback org', () => {
    const org1 = makeRow({
      organizationId: 'o1',
      organizationName: 'Org1',
      dateStart: '2018-01-01',
      dateEnd: '2019-12-31',
    })
    const org2 = makeRow({
      organizationId: 'o2',
      organizationName: 'Org2',
      dateStart: '2021-01-01',
      dateEnd: null,
    })
    const boundaries = [
      startOfDay('2018-01-01'),
      startOfDay('2020-01-01'),
      startOfDay('2021-01-01'),
      startOfDay('2026-01-01'),
    ]

    const result = buildTimeline([org1, org2], null, boundaries)

    expect(result).toEqual([
      {
        organization: 'Org1',
        startDate: startOfDay('2018-01-01').toISOString(),
        endDate: startOfDay('2019-12-31').toISOString(),
      },
      {
        organization: 'Org2',
        startDate: startOfDay('2021-01-01').toISOString(),
        endDate: null,
      },
    ])
  })

  it('extends the fallback org to cover a trailing gap after the last dated org ends', () => {
    const org1 = makeRow({
      organizationId: 'o1',
      organizationName: 'Org1',
      dateStart: '2018-01-01',
      dateEnd: '2019-12-31',
    })
    const fallback = makeRow({ organizationId: 'fb', organizationName: 'Fallback' })
    const boundaries = [
      startOfDay('2018-01-01'),
      startOfDay('2020-01-01'),
      startOfDay('2026-01-01'),
    ]

    const result = buildTimeline([org1], fallback, boundaries)

    expect(result).toEqual([
      {
        organization: 'Org1',
        startDate: startOfDay('2018-01-01').toISOString(),
        endDate: startOfDay('2019-12-31').toISOString(),
      },
      {
        organization: 'Fallback',
        startDate: startOfDay('2020-01-01').toISOString(),
        endDate: null,
      },
    ])
  })

  it('switches winner at the boundary where a higher-member-count org becomes active', () => {
    const low = makeRow({
      organizationId: 'low',
      organizationName: 'LowMember',
      dateStart: '2018-01-01',
      dateEnd: '2021-12-31',
      memberCount: 10,
    })
    const high = makeRow({
      organizationId: 'high',
      organizationName: 'HighMember',
      dateStart: '2020-01-01',
      dateEnd: null,
      memberCount: 100,
    })
    const boundaries = [
      startOfDay('2018-01-01'),
      startOfDay('2020-01-01'),
      startOfDay('2022-01-01'),
      startOfDay('2026-01-01'),
    ]

    const result = buildTimeline([low, high], null, boundaries)

    expect(result).toEqual([
      {
        organization: 'LowMember',
        startDate: startOfDay('2018-01-01').toISOString(),
        endDate: startOfDay('2019-12-31').toISOString(),
      },
      {
        organization: 'HighMember',
        startDate: startOfDay('2020-01-01').toISOString(),
        endDate: null,
      },
    ])
  })

  it('undated org in allRows competes at every boundary and takes over after the dated org ends', () => {
    const undated = makeRow({
      organizationId: 'undated',
      organizationName: 'Undated',
      dateStart: null,
      dateEnd: null,
    })
    const dated = makeRow({
      organizationId: 'dated',
      organizationName: 'Dated',
      dateStart: '2020-01-01',
      dateEnd: '2020-12-31',
    })
    const boundaries = [
      startOfDay('2020-01-01'),
      startOfDay('2021-01-01'),
      startOfDay('2026-01-01'),
    ]

    const result = buildTimeline([undated, dated], null, boundaries)

    // At 2020-01-01: both active, only one has dateStart → Dated wins (rule 3)
    // At 2021-01-01: only Undated active (Dated ended) → switch to Undated
    expect(result).toEqual([
      {
        organization: 'Dated',
        startDate: startOfDay('2020-01-01').toISOString(),
        endDate: startOfDay('2020-12-31').toISOString(),
      },
      {
        organization: 'Undated',
        startDate: startOfDay('2021-01-01').toISOString(),
        endDate: null,
      },
    ])
  })

  it('returns an empty array when no rows are active at any boundary', () => {
    const result = buildTimeline([], null, [startOfDay('2020-01-01'), startOfDay('2026-01-01')])
    expect(result).toEqual([])
  })
})

// ---------------------------------------------------------------------------
// resolveAffiliationsByMemberIds
// ---------------------------------------------------------------------------

describe('resolveAffiliationsByMemberIds', () => {
  let mockSelect: ReturnType<typeof vi.fn>
  let mockQx: QueryExecutor

  beforeEach(() => {
    mockSelect = vi.fn()
    mockQx = { select: mockSelect } as unknown as QueryExecutor
  })

  it('returns an empty affiliations array for a member with no work experiences', async () => {
    mockSelect.mockResolvedValueOnce([]).mockResolvedValueOnce([])

    const result = await resolveAffiliationsByMemberIds(mockQx, ['m1'])

    expect(result.get('m1')).toEqual([])
  })

  it('maps member IDs not present in DB rows to empty arrays', async () => {
    mockSelect.mockResolvedValueOnce([]).mockResolvedValueOnce([])

    const result = await resolveAffiliationsByMemberIds(mockQx, ['m1', 'm2'])

    expect(result.get('m1')).toEqual([])
    expect(result.get('m2')).toEqual([])
  })

  it('resolves a single ongoing work experience into one affiliation period', async () => {
    const row = makeRow({
      memberId: 'm1',
      organizationId: 'org1',
      organizationName: 'Acme',
      dateStart: '2020-01-01',
      dateEnd: null,
    })
    mockSelect.mockResolvedValueOnce([row]).mockResolvedValueOnce([])

    const result = await resolveAffiliationsByMemberIds(mockQx, ['m1'])
    const affiliations = result.get('m1')

    expect(affiliations).toHaveLength(1)
    expect(affiliations[0].organization).toBe('Acme')
    expect(affiliations[0].startDate).toBe(startOfDay('2020-01-01').toISOString())
    expect(affiliations[0].endDate).toBeNull()
  })

  it('filters out work experiences with blacklisted titles', async () => {
    const blacklisted = makeRow({
      memberId: 'm1',
      organizationName: 'Angel Fund',
      dateStart: '2020-01-01',
      title: 'Lead Investor',
    })
    mockSelect.mockResolvedValueOnce([blacklisted]).mockResolvedValueOnce([])

    const result = await resolveAffiliationsByMemberIds(mockQx, ['m1'])

    expect(result.get('m1')).toEqual([])
  })

  it('returns results sorted newest-first', async () => {
    const older = makeRow({
      id: 'we1',
      memberId: 'm1',
      organizationId: 'org1',
      organizationName: 'OldOrg',
      dateStart: '2018-01-01',
      dateEnd: '2019-12-31',
    })
    const newer = makeRow({
      id: 'we2',
      memberId: 'm1',
      organizationId: 'org2',
      organizationName: 'NewOrg',
      dateStart: '2021-01-01',
      dateEnd: '2022-12-31',
    })
    mockSelect.mockResolvedValueOnce([older, newer]).mockResolvedValueOnce([])

    const result = await resolveAffiliationsByMemberIds(mockQx, ['m1'])
    const affiliations = result.get('m1')

    expect(affiliations[0].organization).toBe('NewOrg')
    expect(affiliations[1].organization).toBe('OldOrg')
  })

  it('processes multiple members independently', async () => {
    const m1row = makeRow({
      id: 'r1',
      memberId: 'm1',
      organizationId: 'o1',
      organizationName: 'Acme',
      dateStart: '2020-01-01',
      dateEnd: null,
    })
    const m2row = makeRow({
      id: 'r2',
      memberId: 'm2',
      organizationId: 'o2',
      organizationName: 'CERN',
      dateStart: '2019-01-01',
      dateEnd: null,
    })
    mockSelect.mockResolvedValueOnce([m1row, m2row]).mockResolvedValueOnce([])

    const result = await resolveAffiliationsByMemberIds(mockQx, ['m1', 'm2'])

    expect(result.get('m1')[0].organization).toBe('Acme')
    expect(result.get('m2')[0].organization).toBe('CERN')
  })

  it('manual affiliation (segmentId != null) takes priority over a work experience in the overlapping period', async () => {
    // Work experience: 2018-01-01 → 2021-12-31
    // Manual affiliation: 2020-01-01 → 2021-12-31
    // They overlap from 2020-01-01; in that window the manual must win.
    // Both end in the past so the result is date-independent.
    const workExp = makeRow({
      id: 'we1',
      memberId: 'm1',
      organizationId: 'org-work',
      organizationName: 'WorkOrg',
      dateStart: '2018-01-01',
      dateEnd: '2021-12-31',
      segmentId: null,
    })
    const manual = makeRow({
      id: 'ma1',
      memberId: 'm1',
      organizationId: 'org-manual',
      organizationName: 'ManualOrg',
      dateStart: '2020-01-01',
      dateEnd: '2021-12-31',
      segmentId: 'seg-1',
    })
    mockSelect.mockResolvedValueOnce([workExp]).mockResolvedValueOnce([manual])

    const result = await resolveAffiliationsByMemberIds(mockQx, ['m1'])
    const affiliations = result.get('m1')

    // Newest first:
    //   ManualOrg  2020-01-01 → 2021-12-31  (manual won the overlap)
    //   WorkOrg    2018-01-01 → 2019-12-31  (work exp before manual started)
    expect(affiliations).toHaveLength(2)
    expect(affiliations[0].organization).toBe('ManualOrg')
    expect(affiliations[0].startDate).toBe(startOfDay('2020-01-01').toISOString())
    expect(affiliations[1].organization).toBe('WorkOrg')
    expect(affiliations[1].endDate).toBe(startOfDay('2019-12-31').toISOString())
  })

  it('member with only an undated work experience gets a null-dated affiliation', async () => {
    const undated = makeRow({
      memberId: 'm1',
      organizationId: 'org1',
      organizationName: 'Undated Org',
      dateStart: null,
      dateEnd: null,
    })
    mockSelect.mockResolvedValueOnce([undated]).mockResolvedValueOnce([])

    const result = await resolveAffiliationsByMemberIds(mockQx, ['m1'])
    const affiliations = result.get('m1')

    expect(affiliations).toHaveLength(1)
    expect(affiliations[0]).toEqual({ organization: 'Undated Org', startDate: null, endDate: null })
  })

  // primaryUndated filtering: when one undated work-experience row is marked
  // isPrimaryWorkExperience=true, all other undated work-experience rows are
  // dropped to avoid infinite conflicts. Manual affiliations (segmentId != null)
  // are never dropped regardless.
  it('drops other undated work-experience orgs when one undated primary exists', async () => {
    const primary = makeRow({
      id: 'primary',
      memberId: 'm1',
      organizationId: 'org-primary',
      organizationName: 'PrimaryUndated',
      isPrimaryWorkExperience: true,
      dateStart: null,
      dateEnd: null,
      segmentId: null,
    })
    const extra = makeRow({
      id: 'extra',
      memberId: 'm1',
      organizationId: 'org-extra',
      organizationName: 'ExtraUndated',
      isPrimaryWorkExperience: false,
      dateStart: null,
      dateEnd: null,
      segmentId: null,
    })
    mockSelect.mockResolvedValueOnce([primary, extra]).mockResolvedValueOnce([])

    const result = await resolveAffiliationsByMemberIds(mockQx, ['m1'])
    const affiliations = result.get('m1')

    expect(affiliations).toHaveLength(1)
    expect(affiliations[0].organization).toBe('PrimaryUndated')
  })

  it('keeps manual affiliations even when an undated primary work experience is present', async () => {
    // The primaryUndated filter drops extra undated *work-experience* rows but must
    // never drop manual affiliations (segmentId != null), even if they are also undated.
    // Give the manual a dateStart so it participates in the timeline and appears in the result.
    const primary = makeRow({
      id: 'primary',
      memberId: 'm1',
      organizationId: 'org-primary',
      organizationName: 'PrimaryUndated',
      isPrimaryWorkExperience: true,
      dateStart: null,
      dateEnd: null,
      segmentId: null,
    })
    const extra = makeRow({
      id: 'extra',
      memberId: 'm1',
      organizationId: 'org-extra',
      organizationName: 'ExtraUndated',
      isPrimaryWorkExperience: false,
      dateStart: null,
      dateEnd: null,
      segmentId: null,
    })
    const manual = makeRow({
      id: 'manual',
      memberId: 'm1',
      organizationId: 'org-manual',
      organizationName: 'ManualOrg',
      isPrimaryWorkExperience: false,
      dateStart: '2020-01-01',
      dateEnd: '2021-12-31',
      segmentId: 'seg-1',
    })
    // primary + extra come from the work-experiences query; manual from the manual affiliations query
    mockSelect.mockResolvedValueOnce([primary, extra]).mockResolvedValueOnce([manual])

    const result = await resolveAffiliationsByMemberIds(mockQx, ['m1'])
    const orgs = result.get('m1').map((a) => a.organization)

    expect(orgs).toContain('ManualOrg') // manual survived the filter
    expect(orgs).not.toContain('ExtraUndated') // extra undated work-exp was dropped
  })

  // Error cases
  it('propagates errors thrown by qx.select', async () => {
    mockSelect.mockRejectedValueOnce(new Error('DB connection lost'))

    await expect(resolveAffiliationsByMemberIds(mockQx, ['m1'])).rejects.toThrow(
      'DB connection lost',
    )
  })

  it('propagates errors thrown by the second qx.select (manual affiliations query)', async () => {
    mockSelect.mockResolvedValueOnce([]).mockRejectedValueOnce(new Error('query timeout'))

    await expect(resolveAffiliationsByMemberIds(mockQx, ['m1'])).rejects.toThrow('query timeout')
  })
})
