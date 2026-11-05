import { IActivityScoringGrid } from '@crowd/types'

export enum CredlyActivityType {
  ISSUED_BADGE = 'issued-badge',
  ACCEPTED_BADGE = 'accepted-badge',
  REJECTED_BADGE = 'rejected-badge',
  REVOKED_BADGE = 'revoked-badge',
  REPLACED_BADGE = 'replaced-badge',
  EXPIRED_BADGE = 'expired-badge',
}

export const CREDLY_GRID: Record<CredlyActivityType, IActivityScoringGrid> = {
  [CredlyActivityType.ISSUED_BADGE]: { score: 1 },
  [CredlyActivityType.ACCEPTED_BADGE]: { score: 1 },
  [CredlyActivityType.REJECTED_BADGE]: { score: 0 },
  [CredlyActivityType.REVOKED_BADGE]: { score: 0 },
  [CredlyActivityType.REPLACED_BADGE]: { score: 0 },
  [CredlyActivityType.EXPIRED_BADGE]: { score: 0 },
}
