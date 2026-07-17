export interface ActivityRelationDbRow {
  activityId: string
  memberId: string
  objectMemberId: string | null
  organizationId: string | null
  conversationId: string | null
  parentId: string | null
  segmentId: string
  platform: string
  username: string
  objectMemberUsername: string | null
  createdAt: string
  updatedAt: string
  sourceId: string | null
  sourceParentId: string | null
  type: string | null
  timestamp: string | null
  channel: string | null
  sentimentScore: number | null
  gitInsertions: number | null
  gitDeletions: number | null
  score: number | null
  isContribution: boolean | null
  pullRequestReviewState: string | null
}

export type ActivityRelationDbInsert = Pick<
  ActivityRelationDbRow,
  | 'activityId'
  | 'memberId'
  | 'segmentId'
  | 'platform'
  | 'username'
  | 'sourceId'
  | 'type'
  | 'timestamp'
  | 'channel'
> &
  Partial<
    Omit<
      ActivityRelationDbRow,
      | 'activityId'
      | 'memberId'
      | 'segmentId'
      | 'platform'
      | 'username'
      | 'sourceId'
      | 'type'
      | 'timestamp'
      | 'channel'
      | 'createdAt'
      | 'updatedAt'
      | 'isContribution'
    >
  >
