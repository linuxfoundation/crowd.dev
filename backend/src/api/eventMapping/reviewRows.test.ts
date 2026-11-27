import { describe, expect, it } from 'vitest'

import type { ISessionizeEventForReview } from '@crowd/data-access-layer/src/sessionize'

import { buildEventMappingRows, catalogueEventSuggester } from './reviewRows'
import type { EventMappingMatch, SubprojectForMapping } from './reviewRows'

const EXAMPLE_CON = {
  eventId: 'cvent-100',
  eventName: 'ExampleCon 2027',
  startDate: '2027-03-10',
  projectId: 'PCC-EXAMPLE',
  source: 'cvent',
  dayGap: 0,
}

const SUBPROJECTS: SubprojectForMapping[] = [
  {
    id: 'segment-example',
    name: 'Example Project',
    slug: 'example-project',
    sourceId: 'pcc-example',
    groupName: 'Example Foundation',
  },
  {
    id: 'segment-torch',
    name: 'Torch Project',
    slug: 'ptproject',
    sourceId: null,
    groupName: 'Torch Foundation',
  },
  {
    id: 'segment-other',
    name: 'Other Project',
    slug: 'other-project',
    sourceId: 'pcc-other',
    groupName: 'Other Foundation',
  },
]

function heldEvent(overrides: Partial<ISessionizeEventForReview> = {}): ISessionizeEventForReview {
  return {
    eventKey: 'event-1',
    eventName: 'ExampleCon 2027',
    eventStart: '2027-03-10',
    eventEnd: '2027-03-11',
    status: 'held',
    holdReason: 'no_segment',
    cventEventId: null,
    segmentId: null,
    segmentName: null,
    projectGroupName: null,
    evidence: { candidates: [EXAMPLE_CON], nearby: [EXAMPLE_CON] },
    reviewedAt: null,
    lockedAt: null,
    ...overrides,
  }
}

function rowOf(event: ISessionizeEventForReview, subprojects = SUBPROJECTS) {
  const { rows } = buildEventMappingRows([event], subprojects)
  return rows[0]
}

const EXAMPLE_PROJECT = {
  segmentId: 'segment-example',
  name: 'Example Project',
  groupName: 'Example Foundation',
}

describe('buildEventMappingRows', () => {
  it('suggests the subproject of a same-name Cvent event, matching its source id in any case', () => {
    expect(rowOf(heldEvent())).toEqual({
      platform: 'sessionize',
      eventKey: 'event-1',
      eventName: 'ExampleCon 2027',
      eventStart: '2027-03-10',
      eventEnd: '2027-03-11',
      state: 'suggested',
      match: 'same_name',
      cventEvent: { name: 'ExampleCon 2027', startDate: '2027-03-10', dayGap: 0 },
      suggestedProject: EXAMPLE_PROJECT,
      hintProject: null,
      project: null,
      decidedAt: null,
      canUndo: false,
    })
  })

  it('keeps a not_cvent hold with one resolvable catalogue event as a suggestion', () => {
    const row = rowOf(
      heldEvent({
        holdReason: 'not_cvent',
        evidence: { candidates: [{ ...EXAMPLE_CON, source: 'other-catalogue' }] },
      }),
    )

    expect(row.state).toBe('suggested')
    expect(row.match).toBe('same_name')
    expect(row.suggestedProject).toEqual(EXAMPLE_PROJECT)
  })

  it.each<[EventMappingMatch, Partial<ISessionizeEventForReview>]>([
    [
      'similar_name',
      {
        eventName: 'Example Summit North America 2027',
        holdReason: 'no_match',
        evidence: {
          candidates: [],
          nearby: [{ ...EXAMPLE_CON, eventName: 'Example Summit NA 2027' }],
        },
      },
    ],
    [
      'name_contained',
      {
        eventName: 'Example Cloud Days',
        holdReason: 'no_match',
        evidence: {
          candidates: [],
          nearby: [{ ...EXAMPLE_CON, eventName: 'Example Cloud Days and Partner Expo Europe' }],
        },
      },
    ],
    [
      'sub_event',
      {
        eventName: 'Maintainer Summit: ExampleCon 2027',
        holdReason: 'no_match',
        evidence: { candidates: [], nearby: [EXAMPLE_CON] },
      },
    ],
  ])('suggests the project of a %s Cvent event', (match, overrides) => {
    const row = rowOf(heldEvent(overrides))

    expect(row.match).toBe(match)
    expect(row.state).toBe('suggested')
    expect(row.suggestedProject).toEqual(EXAMPLE_PROJECT)
    expect(row.cventEvent?.name).toBe(
      (overrides.evidence as { nearby: { eventName: string }[] }).nearby[0].eventName,
    )
  })

  it('suggests the single subproject of a project alias', () => {
    const row = rowOf(
      heldEvent({
        eventName: 'Example PyTorch Meetup',
        holdReason: 'no_match',
        evidence: { candidates: [], nearby: [] },
      }),
    )

    expect(row).toMatchObject({
      state: 'suggested',
      match: 'project_alias',
      cventEvent: null,
      suggestedProject: {
        segmentId: 'segment-torch',
        name: 'Torch Project',
        groupName: 'Torch Foundation',
      },
    })
  })

  it.each<[EventMappingMatch, Partial<ISessionizeEventForReview>]>([
    [
      'ambiguous',
      {
        holdReason: 'ambiguous',
        evidence: { candidates: [EXAMPLE_CON, { ...EXAMPLE_CON, eventId: 'cvent-101' }] },
      },
    ],
    ['no_cvent_event', { holdReason: 'no_match', evidence: { candidates: [], nearby: [] } }],
    [
      'main_event_not_in_cdp',
      {
        eventName: 'Maintainer Summit: Unlisted Conference',
        holdReason: 'no_match',
        evidence: { candidates: [], nearby: [] },
      },
    ],
    ['not_checked', { holdReason: 'no_match', evidence: { candidates: [] } }],
  ])('leaves a %s hold unresolved', (match, overrides) => {
    const row = rowOf(heldEvent(overrides))

    expect(row.match).toBe(match)
    expect(row.state).toBe('unresolved')
    expect(row.suggestedProject).toBeNull()
  })

  it('shows the project of an ambiguous hold hint without suggesting it', () => {
    const row = rowOf(
      heldEvent({
        holdReason: 'ambiguous',
        evidence: { candidates: [EXAMPLE_CON, { ...EXAMPLE_CON, eventId: 'cvent-101' }] },
      }),
    )

    expect(row.hintProject).toEqual(EXAMPLE_PROJECT)
    expect(row.cventEvent).toBeNull()
  })

  it('marks a Cvent event whose project has no subproject as not in CDP, keeping the event', () => {
    const row = rowOf(
      heldEvent({
        evidence: { candidates: [{ ...EXAMPLE_CON, projectId: 'pcc-unknown' }] },
      }),
    )

    expect(row).toMatchObject({
      state: 'unresolved',
      match: 'project_not_in_cdp',
      cventEvent: { name: 'ExampleCon 2027', startDate: '2027-03-10', dayGap: 0 },
      suggestedProject: null,
    })
  })

  it('refuses to pick between two subprojects sharing a source id', () => {
    const row = rowOf(heldEvent(), [
      ...SUBPROJECTS,
      { ...SUBPROJECTS[0], id: 'segment-example-copy', sourceId: 'PCC-EXAMPLE' },
    ])

    expect(row.match).toBe('project_not_in_cdp')
    expect(row.suggestedProject).toBeNull()
  })

  it('never resolves an empty project id, even to a subproject with an empty source id', () => {
    const row = rowOf(
      heldEvent({ evidence: { candidates: [{ ...EXAMPLE_CON, projectId: '' }] } }),
      [...SUBPROJECTS, { ...SUBPROJECTS[2], id: 'segment-blank', sourceId: '' }],
    )

    expect(row.match).toBe('project_not_in_cdp')
    expect(row.suggestedProject).toBeNull()
  })

  it('refuses to pick between two subprojects sharing an alias slug', () => {
    const row = rowOf(
      heldEvent({
        eventName: 'Example PyTorch Meetup',
        holdReason: 'no_match',
        evidence: { candidates: [], nearby: [] },
      }),
      [...SUBPROJECTS, { ...SUBPROJECTS[1], id: 'segment-torch-copy' }],
    )

    expect(row.match).toBe('project_not_in_cdp')
    expect(row.state).toBe('unresolved')
  })

  it('shows a reviewed confirmation with its project, undoable until onboarded', () => {
    const confirmed = heldEvent({
      status: 'matched',
      segmentId: 'segment-other',
      segmentName: 'Other Project',
      projectGroupName: 'Other Foundation',
      reviewedAt: '2027-01-05T10:00:00.000Z',
    })

    expect(rowOf(confirmed)).toMatchObject({
      state: 'confirmed',
      match: 'same_name',
      suggestedProject: EXAMPLE_PROJECT,
      project: { segmentId: 'segment-other', name: 'Other Project', groupName: 'Other Foundation' },
      decidedAt: '2027-01-05T10:00:00.000Z',
      canUndo: true,
    })
    expect(rowOf({ ...confirmed, lockedAt: '2027-01-06T05:30:00.000Z' }).canUndo).toBe(false)
  })

  it('shows an ignored event without a project', () => {
    const row = rowOf(heldEvent({ status: 'excluded', reviewedAt: '2027-01-05T10:00:00.000Z' }))

    expect(row).toMatchObject({ state: 'ignored', project: null, canUndo: true })
  })

  it('counts rows per state', () => {
    const { counts } = buildEventMappingRows(
      [
        heldEvent({ eventKey: 'suggested' }),
        heldEvent({ eventKey: 'unresolved', holdReason: 'no_match', evidence: { candidates: [] } }),
        heldEvent({ eventKey: 'ignored', status: 'excluded', reviewedAt: '2027-01-05' }),
        heldEvent({ eventKey: 'ignored-too', status: 'excluded', reviewedAt: '2027-01-05' }),
      ],
      SUBPROJECTS,
    )

    expect(counts).toEqual({ suggested: 1, unresolved: 1, confirmed: 0, ignored: 2 })
  })

  it.each<[string, Partial<ISessionizeEventForReview>, RegExp]>([
    ['evidence without candidates', { evidence: { nearby: [] } }, /malformed evidence/],
    [
      'a candidate without an event id',
      { evidence: { candidates: [{ eventName: 'x' }] } },
      /malformed evidence/,
    ],
    ['an unknown hold reason', { holdReason: 'unknown' }, /malformed hold reason/],
    [
      'a confirmation without a project name',
      { status: 'matched', segmentId: 'segment-other' },
      /no subproject to show/,
    ],
  ])('throws on a row with %s', (_name, overrides, message) => {
    expect(() => rowOf(heldEvent(overrides))).toThrow(message)
  })
})

describe('catalogueEventSuggester', () => {
  const NONE = { cventEventId: null, catalogueSource: null }
  const suggestedCatalogueEvent = catalogueEventSuggester(SUBPROJECTS)

  it('returns the suggested catalogue event only for the suggested segment', () => {
    expect(suggestedCatalogueEvent(heldEvent(), 'segment-example')).toEqual({
      cventEventId: 'cvent-100',
      catalogueSource: 'cvent',
    })
    expect(suggestedCatalogueEvent(heldEvent(), 'segment-other')).toEqual(NONE)
    expect(suggestedCatalogueEvent(undefined, 'segment-example')).toEqual(NONE)
  })

  it('returns neither id nor source for a suggested event without a source', () => {
    const sourceless = heldEvent({
      holdReason: 'not_cvent',
      evidence: { candidates: [{ ...EXAMPLE_CON, source: null }] },
    })

    expect(rowOf(sourceless).suggestedProject).toEqual(EXAMPLE_PROJECT)
    expect(suggestedCatalogueEvent(sourceless, 'segment-example')).toEqual(NONE)
  })

  it('returns neither for a project alias suggestion, which has no catalogue event', () => {
    const alias = heldEvent({
      eventName: 'Example PyTorch Meetup',
      holdReason: 'no_match',
      evidence: { candidates: [], nearby: [] },
    })

    expect(suggestedCatalogueEvent(alias, 'segment-torch')).toEqual(NONE)
  })
})
