import {
  IAttributes,
  IMemberAffiliationMergeBackup,
  IMemberContribution,
  IMemberIdentity,
  IMemberOrganization,
  IMemberReach,
  IMemberRenderFriendlyRole,
  IMemberRoleWithOrganization,
  IMemberUsername,
  IOrganization,
  LLMSuggestionVerdictType,
  MergeActionState,
  MergeActionStep,
  MergeActionType,
} from '.'

export interface IUnmergeBackup<T> {
  primary: T
  secondary: T
}

export interface IUnmergePreviewResult<T> {
  mergeActionId?: string
  primary: T
  secondary: T
}

export interface IMergeAction {
  id: string
  type: MergeActionType
  primaryId: string
  secondaryId: string
  createdAt: string
  updatedAt: string
  step: MergeActionStep
  state: MergeActionState
  unmergeBackup: IUnmergeBackup<IMemberUnmergeBackup | IOrganizationUnmergeBackup>
}

export type IMergeActionColumns = keyof IMergeAction

export interface IMemberUnmergeBackup {
  id: string
  reach: IMemberReach
  joinedAt: string
  tenantId: string
  username: IMemberUsername
  attributes: IAttributes
  identities: IMemberIdentity[]
  displayName: string
  affiliations: IMemberAffiliationMergeBackup[]
  manuallyCreated: boolean
  manuallyChangedFields: string[]
  memberOrganizations: IMemberRoleWithOrganization[]
  contributions: IMemberContribution[]
}

export interface IMemberUnmergePreviewResult {
  id: string
  reach: IMemberReach
  joinedAt: string
  username: IMemberUsername
  attributes: IAttributes
  displayName: string
  affiliations: IMemberAffiliationMergeBackup[]
  contributions: IMemberContribution[]
  manuallyCreated: boolean
  manuallyChangedFields: string[]
  identities: IMemberIdentity[]
  memberOrganizations: IMemberRoleWithOrganization[]
  numberOfOpenSourceContributions: number
  organizations: IMemberRenderFriendlyRole[]
}

export interface IOrganizationUnmergeBackup extends IOrganization {
  memberOrganizations: IMemberOrganization[]
  activityCount: number
  memberCount: number
}

export interface IOrganizationUnmergePreviewResult extends IOrganization {
  memberCount: number
  activityCount: number
}

export interface MemberRow {
  id: string
  displayName: string
  joinedAt: string
  attributes: Record<string, unknown>
  reach: Record<string, number>
  contributions: IMemberContribution[] | null
  manuallyCreated: boolean
  manuallyChangedFields: string[] | null
  score: number | null
  tenantId: string
  createdAt: string
  updatedAt: string
}

export interface MemberUnmergeResult {
  primary: IMemberUnmergePreviewResult
  secondary: IMemberUnmergePreviewResult
  movedIdentities: IMemberIdentity[]
}

export interface ILLMSuggestionVerdict {
  id?: string
  type: LLMSuggestionVerdictType
  model: string
  primaryId: string
  secondaryId: string
  prompt: string
  verdict: string
  inputTokenCount: number
  outputTokenCount: number
  responseTimeSeconds: number
  createdAt?: string
}
