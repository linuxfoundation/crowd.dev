import { IActivityScoringGrid } from '@crowd/types'

export enum CventActivityType {
  REGISTERED_EVENT = 'registered-event',
  ATTENDED_EVENT = 'attended-event',
}

export const CVENT_GRID: Record<CventActivityType, IActivityScoringGrid> = {
  [CventActivityType.ATTENDED_EVENT]: {
    score: 8,
  },
  [CventActivityType.REGISTERED_EVENT]: {
    score: 3,
  },
}
