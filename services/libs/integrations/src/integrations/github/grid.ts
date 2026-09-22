import { IActivityScoringGrid } from '@crowd/types'

import { GithubActivityType } from './types'

export const GITHUB_GRID: Record<GithubActivityType, IActivityScoringGrid> = {
  [GithubActivityType.DISCUSSION_STARTED]: {
    score: 8,
  },
  [GithubActivityType.DISCUSSION_COMMENT]: {
    score: 6,
  },
  [GithubActivityType.FORK]: {
    score: 4,
  },
  [GithubActivityType.ISSUE_CLOSED]: {
    score: 6,
  },
  [GithubActivityType.ISSUE_OPENED]: {
    score: 8,
  },
  [GithubActivityType.ISSUE_COMMENT]: {
    score: 6,
  },
  [GithubActivityType.PULL_REQUEST_CLOSED]: {
    score: 8,
  },
  [GithubActivityType.PULL_REQUEST_OPENED]: {
    score: 10,
  },
  [GithubActivityType.PULL_REQUEST_COMMENT]: {
    score: 6,
  },
  [GithubActivityType.PULL_REQUEST_MERGED]: {
    score: 6,
  },
  [GithubActivityType.PULL_REQUEST_ASSIGNED]: {
    score: 2,
  },
  [GithubActivityType.PULL_REQUEST_REVIEWED]: {
    score: 8,
  },
  [GithubActivityType.PULL_REQUEST_REVIEW_REQUESTED]: {
    score: 2,
  },
  [GithubActivityType.PULL_REQUEST_REVIEW_THREAD_COMMENT]: {
    score: 6,
  },
  [GithubActivityType.AUTHORED_COMMIT]: {
    score: 2,
  },
}
