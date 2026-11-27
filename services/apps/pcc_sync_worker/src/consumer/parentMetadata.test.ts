import { describe, expect, it } from 'vitest'

import { SegmentStatus } from '@crowd/types'

import type { PccParentMetadata } from '../parser'
import {
  addParents,
  effectiveParentMetadataMode,
  planParentMetadata,
  readParentMetadataMode,
} from './parentMetadata'
import type { CdpParentInsightsProject, CdpParentSegment } from './parentMetadata'

const CNCF_IN_PCC: PccParentMetadata = {
  pccProjectId: 'pcc-cncf',
  name: 'CNCF',
  status: SegmentStatus.ACTIVE,
  maturity: 'Graduated',
  description: 'Cloud native computing',
  logoUrl: 'https://logos.test/cncf.svg',
}

function cncfSegment(overrides: Partial<CdpParentSegment> = {}): CdpParentSegment {
  return {
    id: 'segment-cncf',
    type: 'projectGroup',
    name: 'CNCF',
    sourceId: CNCF_IN_PCC.pccProjectId,
    status: SegmentStatus.ACTIVE,
    maturity: 'Graduated',
    description: 'Cloud native computing',
    ...overrides,
  }
}

function cncfInsightsProject(
  overrides: Partial<CdpParentInsightsProject> = {},
): CdpParentInsightsProject {
  return {
    id: 'insights-cncf',
    segmentId: 'segment-cncf',
    description: 'Cloud native computing',
    logoUrl: 'https://logos.test/cncf.svg',
    ...overrides,
  }
}

function parents(...list: PccParentMetadata[]): Map<string, PccParentMetadata> {
  const byId = new Map<string, PccParentMetadata>()
  addParents(byId, list)
  return byId
}

describe('readParentMetadataMode', () => {
  it('is off when nothing is set', () => {
    expect(readParentMetadataMode({})).toBe('off')
  })

  it.each(['on', 'dry-run'] as const)('reads %s', (mode) => {
    expect(readParentMetadataMode({ PCC_PARENT_METADATA_SYNC: mode })).toBe(mode)
  })

  it('stays off for a value it does not know', () => {
    expect(readParentMetadataMode({ PCC_PARENT_METADATA_SYNC: 'true' })).toBe('off')
  })
})

describe('effectiveParentMetadataMode', () => {
  it.each([
    { mode: 'off', syncIsDryRun: true, expected: 'off' },
    { mode: 'dry-run', syncIsDryRun: false, expected: 'dry-run' },
    { mode: 'on', syncIsDryRun: false, expected: 'on' },
    { mode: 'on', syncIsDryRun: true, expected: 'dry-run' },
  ] as const)(
    '$mode with sync dry run $syncIsDryRun is $expected',
    ({ mode, syncIsDryRun, expected }) => {
      expect(effectiveParentMetadataMode(mode, syncIsDryRun)).toBe(expected)
    },
  )
})

describe('addParents', () => {
  it('keeps one entry for a parent seen under several projects', () => {
    const byId = parents(CNCF_IN_PCC, { ...CNCF_IN_PCC, description: 'seen again' })

    expect([...byId.values()]).toEqual([CNCF_IN_PCC])
  })
})

describe('planParentMetadata', () => {
  it('plans nothing when CDP already holds the PCC values', () => {
    const plan = planParentMetadata(parents(CNCF_IN_PCC), [cncfSegment()], [cncfInsightsProject()])

    expect(plan).toEqual({ segments: [], insightsProjects: [] })
  })

  it('fills a parent created without metadata', () => {
    const plan = planParentMetadata(
      parents(CNCF_IN_PCC),
      [cncfSegment({ maturity: null, description: null })],
      [cncfInsightsProject({ description: null, logoUrl: null })],
    )

    expect(plan.segments).toEqual([
      {
        segmentId: 'segment-cncf',
        segmentType: 'projectGroup',
        segmentName: 'CNCF',
        pccProjectId: 'pcc-cncf',
        status: undefined,
        maturity: { from: null, to: 'Graduated' },
        description: { from: null, to: 'Cloud native computing' },
      },
    ])
    expect(plan.insightsProjects).toEqual([
      {
        insightsProjectId: 'insights-cncf',
        segmentId: 'segment-cncf',
        pccProjectId: 'pcc-cncf',
        description: { from: null, to: 'Cloud native computing' },
        logoUrl: { from: null, to: 'https://logos.test/cncf.svg' },
      },
    ])
  })

  it('replaces a status and a description that differ from PCC', () => {
    const plan = planParentMetadata(
      parents({ ...CNCF_IN_PCC, status: SegmentStatus.ARCHIVED }),
      [cncfSegment({ description: 'Typed by hand' })],
      [],
    )

    expect(plan.segments).toMatchObject([
      {
        status: { from: SegmentStatus.ACTIVE, to: SegmentStatus.ARCHIVED },
        description: { from: 'Typed by hand', to: 'Cloud native computing' },
      },
    ])
  })

  it('never replaces a CDP value with an empty PCC value', () => {
    const emptyInPcc = { ...CNCF_IN_PCC, status: null, maturity: null, description: null }

    const plan = planParentMetadata(
      parents({ ...emptyInPcc, logoUrl: null }),
      [cncfSegment()],
      [cncfInsightsProject()],
    )

    expect(plan).toEqual({ segments: [], insightsProjects: [] })
  })

  it('keeps a logo that the Insights project already has', () => {
    const plan = planParentMetadata(
      parents({ ...CNCF_IN_PCC, logoUrl: 'https://logos.test/new.svg' }),
      [cncfSegment()],
      [cncfInsightsProject()],
    )

    expect(plan.insightsProjects).toEqual([])
  })

  it('plans no logo that the update would not write over an empty one', () => {
    const plan = planParentMetadata(
      parents(CNCF_IN_PCC),
      [cncfSegment()],
      [cncfInsightsProject({ logoUrl: '' })],
    )

    expect(plan.insightsProjects).toEqual([])
  })

  it('updates every level that carries the PCC id of the parent', () => {
    const plan = planParentMetadata(
      parents(CNCF_IN_PCC),
      [
        cncfSegment({ description: null }),
        cncfSegment({ id: 'segment-cncf-project', type: 'project', description: null }),
      ],
      [],
    )

    expect(plan.segments.map((update) => update.segmentType)).toEqual(['projectGroup', 'project'])
  })

  it('ignores a segment whose PCC id is not a parent of this export', () => {
    const plan = planParentMetadata(
      parents(CNCF_IN_PCC),
      [cncfSegment({ sourceId: 'pcc-other', description: null })],
      [cncfInsightsProject({ description: null })],
    )

    expect(plan).toEqual({ segments: [], insightsProjects: [] })
  })
})
