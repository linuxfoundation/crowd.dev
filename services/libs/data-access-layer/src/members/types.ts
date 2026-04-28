import { IAttributes, IMemberAttribute, MemberAttributeType } from '@crowd/types'

export interface IQueryNumberOfNewMembers {
  segmentIds?: string[]
  after?: Date
  before?: Date
  platform?: string
}

export interface IQueryTimeseriesOfNewMembers {
  segmentIds?: string[]
  after: Date
  before: Date
  platform?: string
}

export interface IMemberSegmentCoreAggregates {
  memberId: string
  segmentId: string

  activityCount: number
  activeOn: string[]
}

export interface IMemberSegmentDisplayAggregates {
  memberId: string
  segmentId: string

  lastActive: string
  activityTypes: string[]
  averageSentiment: number
}

export interface IMemberSegmentAggregates
  extends IMemberSegmentCoreAggregates,
    IMemberSegmentDisplayAggregates {}

export interface IMemberActivitySummary {
  activityCount: number
  lastActive: string
}

export interface AttributeData extends IMemberAttribute {
  id: string
  createdAt: string
  updatedAt: string
}

export interface IDbMemberAttributeSetting {
  id: string
  type: MemberAttributeType
  canDelete: boolean
  show: boolean
  label: string
  name: string
  options: unknown
}

export interface IDbMemberTag {
  id: string
}

export interface IDbMemberData {
  id: string
  tags: IDbMemberTag[]
  displayName: string

  activityCount?: number
  lastActive?: string
  activityTypes?: string[]
  activeOn?: string[]
  averageSentiment?: number
  activeDaysCount?: number
}

export interface IDbMemberBotSuggestion {
  id: string
  memberId: string
  confidence: number
  createdAt: string
}

export type IDbMemberBotSuggestionInsert = Omit<
  IDbMemberBotSuggestion,
  'id' | 'createdAt' | 'updatedAt'
>

export interface IDbMemberBotSuggestionBySegment {
  activityCount: number
  memberId: string
  confidence: number
  displayName: string
  avatarUrl: string
  attributes: IAttributes
}
