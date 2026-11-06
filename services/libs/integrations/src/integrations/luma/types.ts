import { IActivityScoringGrid } from '@crowd/types'

export enum LumaActivityType {
  REGISTERED_EVENT = 'registered-event',
  ATTENDED_EVENT = 'attended-event',
}

export const LUMA_GRID: Record<LumaActivityType, IActivityScoringGrid> = {
  [LumaActivityType.REGISTERED_EVENT]: {
    score: 3,
  },
  [LumaActivityType.ATTENDED_EVENT]: {
    score: 8,
  },
}
