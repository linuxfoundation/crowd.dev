import { describe, expect, it } from 'vitest'

import { IMemberOrganizationData, OrganizationIdentityType, OrganizationSource } from '@crowd/types'

import { IMemberEnrichmentDataNormalizedOrganization } from '../types'

import {
  hasMemberOrganizationTimelineChange,
  prepareWorkExperiences,
} from './workExperienceReconciliation'

function oldRow(overrides: Partial<IMemberOrganizationData> = {}): IMemberOrganizationData {
  return {
    id: 'row-1',
    orgId: 'org-1',
    jobTitle: 'Engineer',
    dateStart: '2020-01-01',
    dateEnd: null,
    source: OrganizationSource.ENRICHMENT_PROGAI,
    verified: false,
    verifiedBy: null,
    ...overrides,
  }
}

function newEntry(
  overrides: Partial<IMemberEnrichmentDataNormalizedOrganization> = {},
): IMemberEnrichmentDataNormalizedOrganization {
  const organizationId = overrides.organizationId ?? 'org-1'
  return {
    organizationId,
    name: 'Org One',
    title: 'Engineer',
    startDate: '2020-01-01',
    endDate: null,
    source: OrganizationSource.ENRICHMENT_PROGAI,
    identities: [
      {
        organizationId,
        platform: 'linkedin',
        value: 'org-one',
        type: OrganizationIdentityType.USERNAME,
        verified: true,
      },
    ],
    ...overrides,
  }
}

describe('prepareWorkExperiences', () => {
  it('leaves a verified row untouched when the provider resupplies a matching entry', () => {
    const verified = oldRow({ id: 'row-verified', verified: true, verifiedBy: 'jane' })
    const result = prepareWorkExperiences([verified], [newEntry()], false, new Set())

    expect(result.toDelete).not.toContain(verified)
    expect(result.toUpdate.has(verified)).toBe(false)
  })

  it('leaves a verified row untouched even when the provider sends conflicting dates', () => {
    const verified = oldRow({ id: 'row-verified', verified: true, verifiedBy: 'jane' })
    const conflicting = newEntry({ startDate: '2021-06-01', endDate: '2022-01-01' })
    const result = prepareWorkExperiences([verified], [conflicting], false, new Set())

    expect(result.toDelete).not.toContain(verified)
    expect(result.toUpdate.has(verified)).toBe(false)
  })

  it('does not insert a duplicate affiliation for a provider entry matching a verified org+title', () => {
    const verified = oldRow({ id: 'row-verified', verified: true, verifiedBy: 'jane' })
    const duplicate = newEntry({ startDate: '2021-06-01', endDate: '2022-01-01' })
    const result = prepareWorkExperiences([verified], [duplicate], false, new Set())

    expect(result.toCreate).toEqual([])
  })

  it('still creates an entry for a distinct role at the same org as a verified row', () => {
    const verified = oldRow({ id: 'row-verified', verified: true, verifiedBy: 'jane' })
    const distinctRole = newEntry({ title: 'Manager', startDate: '2022-01-01', endDate: null })
    const result = prepareWorkExperiences([verified], [distinctRole], false, new Set())

    expect(result.toCreate).toEqual([distinctRole])
  })

  it('never recreates an organization a person deleted on purpose (tombstoned)', () => {
    const tombstonedEntry = newEntry({ organizationId: 'org-deleted' })
    const result = prepareWorkExperiences([], [tombstonedEntry], false, new Set(['org-deleted']))

    expect(result.toCreate).toEqual([])
    expect(result.toDelete).toEqual([])
    expect(result.toUpdate.size).toBe(0)
  })

  it('soft-deletes an enrichment-owned row the provider no longer supplies, but keeps protected rows', () => {
    const uiRow = oldRow({ id: 'row-ui', orgId: 'org-ui', source: OrganizationSource.UI })
    const droppedRow = oldRow({ id: 'row-dropped', orgId: 'org-dropped' })
    const result = prepareWorkExperiences([uiRow, droppedRow], [], false, new Set())

    expect(result.toDelete).toEqual([droppedRow])
    expect(result.toCreate).toEqual([])
    expect(result.toUpdate.size).toBe(0)
  })

  it('makes no writes when the payload matches the existing rows exactly', () => {
    const existing = oldRow()
    const result = prepareWorkExperiences([existing], [newEntry()], false, new Set())

    expect(result.toCreate).toEqual([])
    expect(result.toDelete).toEqual([])
    expect(result.toUpdate.size).toBe(0)
  })

  it('updates only the changed fields when the payload shifts a date', () => {
    const existing = oldRow()
    const shifted = newEntry({ endDate: '2021-12-31' })
    const result = prepareWorkExperiences([existing], [shifted], false, new Set())

    expect(result.toCreate).toEqual([])
    expect(result.toDelete).toEqual([])
    expect(result.toUpdate.get(existing)).toEqual({ dateEnd: '2021-12-31' })
  })

  it('adopts the incoming source on a matched row when the provider source changed', () => {
    const existing = oldRow({ source: OrganizationSource.ENRICHMENT_PROGAI })
    const resupplied = newEntry({ source: OrganizationSource.ENRICHMENT_CRUSTDATA })
    const result = prepareWorkExperiences([existing], [resupplied], false, new Set())

    expect(result.toUpdate.get(existing)).toEqual({
      source: OrganizationSource.ENRICHMENT_CRUSTDATA,
    })
  })

  it('does not treat a date-only vs full-timestamp difference as a change', () => {
    const existing = oldRow({ dateStart: '2020-01-01T00:00:00.000Z', dateEnd: null })
    const resupplied = newEntry({ startDate: '2020-01-01', endDate: null })
    const result = prepareWorkExperiences([existing], [resupplied], false, new Set())

    expect(result.toUpdate.size).toBe(0)
  })

  it('fills a UI row null dateEnd from a matching provider entry', () => {
    const uiRow = oldRow({ id: 'row-ui', source: OrganizationSource.UI, dateEnd: null })
    const matching = newEntry({ endDate: '2021-01-01' })
    const result = prepareWorkExperiences([uiRow], [matching], false, new Set())

    expect(result.toUpdate.get(uiRow)).toEqual({ dateEnd: '2021-01-01' })
    expect(result.toCreate).toEqual([])
    expect(result.toDelete).toEqual([])
  })

  it('drops a provider entry that conflicts with a manually-set UI dateEnd instead of applying it', () => {
    const uiRow = oldRow({ id: 'row-ui', source: OrganizationSource.UI, dateEnd: '2021-06-01' })
    const conflicting = newEntry({ startDate: '2020-01-01', endDate: '2022-01-01' })
    const result = prepareWorkExperiences([uiRow], [conflicting], false, new Set())

    expect(result.toCreate).toEqual([])
    expect(result.toDelete).toEqual([])
    expect(result.toUpdate.size).toBe(0)
  })

  describe('when isHighConfidenceSourceSelectedForWorkExperiences is true', () => {
    it('still filters out tombstoned organizations', () => {
      const tombstonedEntry = newEntry({ organizationId: 'org-deleted' })
      const result = prepareWorkExperiences([], [tombstonedEntry], true, new Set(['org-deleted']))

      expect(result.toCreate).toEqual([])
    })

    it('excludes entries that duplicate an existing UI-entered work experience', () => {
      const uiRow = oldRow({ id: 'row-ui', source: OrganizationSource.UI })
      const duplicate = newEntry()
      const result = prepareWorkExperiences([uiRow], [duplicate], true, new Set())

      expect(result.toCreate).toEqual([])
      expect(result.toDelete).toEqual([])
      expect(result.toUpdate.size).toBe(0)
    })

    it('creates entries that do not overlap with any UI work experience', () => {
      const freshEntry = newEntry({ organizationId: 'org-2', title: 'Manager' })
      const result = prepareWorkExperiences([], [freshEntry], true, new Set())

      expect(result.toCreate).toEqual([freshEntry])
    })
  })
})

describe('hasMemberOrganizationTimelineChange', () => {
  it('returns false when the delete and create sets cover the same organization and dates', () => {
    const toDelete = [oldRow({ orgId: 'org-1', dateStart: '2020-01-01', dateEnd: '2021-01-01' })]
    const toCreate = [
      newEntry({
        organizationId: 'org-1',
        title: 'Different title, same timeline',
        startDate: '2020-01-01',
        endDate: '2021-01-01',
      }),
    ]

    expect(hasMemberOrganizationTimelineChange(toDelete, toCreate)).toBe(false)
  })

  it('returns true when the create set introduces a different timeline', () => {
    const toDelete = [oldRow({ orgId: 'org-1', dateStart: '2020-01-01', dateEnd: '2021-01-01' })]
    const toCreate = [
      newEntry({ organizationId: 'org-1', startDate: '2020-01-01', endDate: '2022-06-01' }),
    ]

    expect(hasMemberOrganizationTimelineChange(toDelete, toCreate)).toBe(true)
  })

  it('returns true when the number of affiliations changes', () => {
    const toDelete = [oldRow({ orgId: 'org-1', dateStart: '2020-01-01', dateEnd: '2021-01-01' })]
    const toCreate = [
      newEntry({ organizationId: 'org-1', startDate: '2020-01-01', endDate: '2021-01-01' }),
      newEntry({ organizationId: 'org-2', startDate: '2021-01-02', endDate: null }),
    ]

    expect(hasMemberOrganizationTimelineChange(toDelete, toCreate)).toBe(true)
  })
})
