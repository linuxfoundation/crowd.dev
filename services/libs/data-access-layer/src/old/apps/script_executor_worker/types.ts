export interface ISimilarMember {
  primaryMemberId: string
  secondaryMemberId: string
  primaryMemberIdentityValue: string
  secondaryMemberIdentityValue: string
  hash: number
}

export interface IFindMemberMergeActionReplacement {
  memberId: string
  startDate?: string
  endDate?: string
  userId?: string
  limit?: number
}

export interface IFindMemberIdentitiesGroupedByPlatformResult {
  ids: string[]
  platforms: string[]
  types: string[]
  verified: boolean[]
  values: string[]
  groupedByValue: string
}

export enum EntityType {
  MEMBER = 'member',
  ORGANIZATION = 'organization',
}

export interface IDuplicateMembersToCleanup {
  primaryId: string
  secondaryId: string
}
