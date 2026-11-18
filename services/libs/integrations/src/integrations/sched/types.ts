import { IActivityScoringGrid } from '@crowd/types'

export enum SchedActivityType {
  REGISTERED_SESSION = 'registered-session',
  SPOKE_AT_SESSION = 'spoke-at-session',
}

export const SCHED_GRID: Record<SchedActivityType, IActivityScoringGrid> = {
  [SchedActivityType.REGISTERED_SESSION]: {
    score: 3,
  },
  [SchedActivityType.SPOKE_AT_SESSION]: {
    score: 8,
  },
}
