import { IActivityRelationDuplicateGroup } from '@crowd/data-access-layer'

export interface IFindAndMergeMembersWithSameVerifiedEmailsInDifferentPlatformsArgs {
  afterHash?: number
}

export interface IFindAndMergeMembersWithSameIdentitiesDifferentCapitalizationInPlatformArgs {
  platform: string
  afterHash?: number
}

export interface IDissectMemberArgs {
  memberId: string
  userId?: string
  startDate?: string
  endDate?: string
  undoActionPerWorkflow?: number
  forceSplitAllIdentities?: boolean
}

export interface IPopulateActivityRelationsArgs {
  batchSizePerRun: number
  latestSyncedActivityTimestamp?: string
  segmentIds?: string[]
}

export interface IScriptBatchTestArgs {
  batchSize?: number
  testRun?: boolean
  cleanupRunId?: string
  cumulativeOrphansFound?: number
  cumulativeOrphansDeleted?: number
  workflowStartTime?: number
}

export interface IFixActivityForiegnKeysArgs extends IScriptBatchTestArgs {
  entityType: string
  offset?: number
}

export interface IProcessLLMVerifiedMergesArgs extends IScriptBatchTestArgs {
  type: string
}

export interface ICleanupDuplicateMembersArgs extends IScriptBatchTestArgs {
  cutoffDate?: string
  checkByActivityIdentity?: boolean
  checkByTwitterIdentity?: boolean
}

export interface IDedupActivityRelationsArgs extends IScriptBatchTestArgs {
  groupsPerRun?: number
  cursor?: Omit<IActivityRelationDuplicateGroup, 'activityIds'>
}

export interface IBlockOrganizationAffiliationArgs extends IScriptBatchTestArgs {
  afterId?: string
}
