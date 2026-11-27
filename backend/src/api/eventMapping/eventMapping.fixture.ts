import type { ISubprojectForMapping } from '@crowd/data-access-layer/src/segments'
import type { ISessionizeEventForReview } from '@crowd/data-access-layer/src/sessionize'

export const EXAMPLE_SEGMENT_ID = '4f2b8c1e-7a3d-4e5f-9b6a-1c2d3e4f5a6b'
export const OTHER_SEGMENT_ID = '9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d'

export const SUBPROJECTS: ISubprojectForMapping[] = [
  {
    id: EXAMPLE_SEGMENT_ID,
    name: 'Example Project',
    slug: 'example-project',
    sourceId: 'pcc-example',
    groupName: 'Example Foundation',
  },
  {
    id: OTHER_SEGMENT_ID,
    name: 'Other Project',
    slug: 'other-project',
    sourceId: 'pcc-other',
    groupName: 'Other Foundation',
  },
]

export function heldEvent(eventKey: string): ISessionizeEventForReview {
  const exampleCon = {
    eventId: `cvent-${eventKey}`,
    eventName: 'ExampleCon 2027',
    startDate: '2027-03-10',
    projectId: 'PCC-EXAMPLE',
    source: 'cvent',
    dayGap: 0,
  }
  return {
    eventKey,
    eventName: 'ExampleCon 2027',
    eventStart: '2027-03-10',
    eventEnd: '2027-03-11',
    status: 'held',
    holdReason: 'no_segment',
    cventEventId: null,
    segmentId: null,
    segmentName: null,
    projectGroupName: null,
    evidence: { candidates: [exampleCon], nearby: [exampleCon] },
    reviewedAt: null,
    lockedAt: null,
  }
}
