import { IActivityScoringGrid } from '@crowd/types'

import { HackerNewsActivityType } from './types'

export const HACKERNEWS_GRID: Record<HackerNewsActivityType, IActivityScoringGrid> = {
  [HackerNewsActivityType.POST]: {
    score: 10,
  },
  [HackerNewsActivityType.COMMENT]: {
    score: 6,
  },
  [HackerNewsActivityType.MENTION]: {
    score: 6,
  },
}
