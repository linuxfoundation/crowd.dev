import { IActivityScoringGrid } from '@crowd/types'

export enum SessionizeActivityType {
  SUBMITTED_SESSION = 'submitted-session',
  SPOKE_AT_SESSION = 'spoke-at-session',
}

export const SESSIONIZE_GRID: Record<SessionizeActivityType, IActivityScoringGrid> = {
  [SessionizeActivityType.SUBMITTED_SESSION]: { score: 3 },
  [SessionizeActivityType.SPOKE_AT_SESSION]: { score: 8 },
}
