import { IActivityScoringGrid } from '@crowd/types'

export enum MeetingsActivityType {
  INVITED_MEETING = 'invited-meeting',
  ATTENDED_MEETING = 'attended-meeting',
}

export const MEETINGS_GRID: Record<MeetingsActivityType, IActivityScoringGrid> = {
  [MeetingsActivityType.INVITED_MEETING]: { score: 1 },
  [MeetingsActivityType.ATTENDED_MEETING]: { score: 1 },
}
