import { ISentimentAnalysisResult, PlatformType } from '@crowd/types'

export interface IDbActivity {
  id: string
  platform: string
  type: string
  timestamp: string
  score: number
  sourceId: string
  sourceParentId?: string
  parentId?: string
  memberId: string
  username: string
  objectMemberId?: string
  objectMemberUsername?: string
  attributes: Record<string, unknown>
  importHash?: string
  body?: string
  title?: string
  channel?: string
  url?: string
  sentiment: ISentimentAnalysisResult
  organizationId?: string
  deletedAt?: string
  createdAt?: string
}

export interface IDbActivityCreateData {
  id?: string
  type: string
  timestamp: string
  platform: string
  score: number
  sourceId: string
  sourceParentId?: string
  importHash?: string
  parentId?: string
  segmentId?: string
  memberId: string
  username: string
  sentiment?: ISentimentAnalysisResult
  attributes: Record<string, unknown>
  body?: string
  title?: string
  channel?: string
  url?: string
  organizationId?: string
  objectMemberId?: string
  objectMemberUsername?: string
  isTeamMemberActivity?: boolean
  isBotActivity?: boolean
  conversationId?: string
  createdById?: string
  updatedById?: string
  createdAt?: string
  updatedAt?: string
  deletedAt?: string
  tenantId?: string
  sentimentScore?: number
  gitInsertions?: number
  gitDeletions?: number
}

export interface IActivityRelationCreateOrUpdateData {
  activityId: string
  memberId: string
  objectMemberId?: string
  organizationId?: string
  conversationId?: string
  parentId?: string
  segmentId: string
  platform: string
  username: string
  objectMemberUsername?: string
  sourceId: string
  sourceParentId?: string
  type: string
  timestamp: string
  channel: string
  sentimentScore: number
  gitInsertions: number
  gitDeletions: number
  score: number
  pullRequestReviewState?: string
}

export interface IActivityRelationUpdateById {
  activityId: string
  memberId?: string
  objectMemberId?: string
  organizationId?: string
  conversationId?: string
  parentId?: string
  segmentId?: string
  platform?: string
  username?: string
  objectMemberUsername?: string
}

export interface IDbActivityUpdateData {
  type: string
  score: number
  parentId?: string
  sourceId: string
  sourceParentId?: string
  segmentId?: string
  memberId: string
  username: string
  objectMemberId?: string
  objectMemberUsername?: string
  sentiment?: ISentimentAnalysisResult
  attributes?: Record<string, unknown>
  body?: string
  title?: string
  channel?: string
  url?: string
  organizationId?: string
  conversationId?: string
  platform?: PlatformType
  isTeamMemberActivity?: boolean
  isBotActivity?: boolean
  updatedById?: string
  updatedAt?: string
  createdAt?: string
  tenantId?: string
}
