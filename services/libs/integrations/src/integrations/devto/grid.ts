import { IActivityScoringGrid } from '@crowd/types'

import { DevToActivityType } from './types'

export const DEVTO_GRID: Record<DevToActivityType, IActivityScoringGrid> = {
  [DevToActivityType.COMMENT]: {
    score: 6,
  },
  [DevToActivityType.MENTION]: {
    score: 6,
  },
}
