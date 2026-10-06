import { IActivityScoringGrid } from '@crowd/types'

import { LinkedinActivityType } from './types'

export const LINKEDIN_GRID: Record<LinkedinActivityType, IActivityScoringGrid> = {
  [LinkedinActivityType.COMMENT]: {
    score: 6,
  },
  [LinkedinActivityType.REACTION]: {
    score: 1,
  },
  [LinkedinActivityType.MENTION]: {
    score: 6,
  },
}
