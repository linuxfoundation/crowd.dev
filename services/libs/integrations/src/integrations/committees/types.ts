import { IActivityScoringGrid } from '@crowd/types'

export enum CommitteesActivityType {
  ADDED_TO_COMMITTEE = 'added-to-committee',
  REMOVED_FROM_COMMITTEE = 'removed-from-committee',
}

export const COMMITTEES_GRID: Record<CommitteesActivityType, IActivityScoringGrid> = {
  [CommitteesActivityType.ADDED_TO_COMMITTEE]: { score: 1 },
  [CommitteesActivityType.REMOVED_FROM_COMMITTEE]: { score: 1 },
}
