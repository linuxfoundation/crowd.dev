export enum SimilarityScoreRange {
  LOW = 'low',
  MEDIUM = 'medium',
  HIGH = 'high',
}

export interface IFetchOrganizationMergeSuggestionFilter {
  similarity?: SimilarityScoreRange[]
  displayName?: string
  organizationId?: string
  projectIds?: string[]
  subprojectIds?: string[]
}

export interface IFetchMemberMergeSuggestionFilter {
  similarity?: SimilarityScoreRange[]
  displayName?: string
  memberId?: string
  projectIds?: string[]
  subprojectIds?: string[]
}

export interface IFetchMemberMergeSuggestionArgs {
  filter?: IFetchMemberMergeSuggestionFilter
  segmentId?: string
  limit: number
  offset: number
  orderBy?: string[]
  detail: boolean
  countOnly: boolean
}

export interface IFetchOrganizationMergeSuggestionArgs {
  filter?: IFetchOrganizationMergeSuggestionFilter
  segmentId?: string
  limit: number
  offset: number
  orderBy?: string[]
  detail: boolean
  countOnly: boolean
}
