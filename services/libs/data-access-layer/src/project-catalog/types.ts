export const PROJECT_CATALOG_ACTIONS = [
  'auto',
  'evaluate',
  'ask',
  'review',
  'onboard',
  'onboarded',
  'skip',
  'unsure',
  'error',
] as const

export type ProjectCatalogAction = (typeof PROJECT_CATALOG_ACTIONS)[number]

export type ProjectCatalogActionCounts = Record<ProjectCatalogAction, number>

export const PROJECT_CATALOG_CLASSIFICATION_KINDS = [
  'non_github_source',
  'non_lf_new_project',
  'lf_not_in_pcc',
  'lf_not_in_cdp',
  'lf_in_cdp',
  'already_tracked',
  'ambiguous',
] as const

export type ProjectCatalogClassificationKind = (typeof PROJECT_CATALOG_CLASSIFICATION_KINDS)[number]

export const REVIEW_REJECTION_SKIP_REASON_PREFIX = 'rejected in review by '

export function reviewRejectionSkipReason(reviewedBy: string): string {
  return `${REVIEW_REJECTION_SKIP_REASON_PREFIX}${reviewedBy}`
}

export function isReviewRejectionSkipReason(skipReason: string | null): boolean {
  return !!skipReason && skipReason.startsWith(REVIEW_REJECTION_SKIP_REASON_PREFIX)
}

export const PROJECT_CATALOG_PROPOSED_ACTIONS = [
  'create_segment',
  'create_integration',
  'update_integration',
  'human_review',
] as const

export type ProjectCatalogProposedAction = (typeof PROJECT_CATALOG_PROPOSED_ACTIONS)[number]

export type ProjectCatalogRoutedAction = Extract<
  ProjectCatalogAction,
  'evaluate' | 'ask' | 'review' | 'onboard' | 'skip'
>

export interface IRequestHintProject {
  projectId: string
  name: string
  slug: string
  isLeaf: boolean
}

export interface IRequestHints {
  declaredLf?: boolean
  treatAsNew?: boolean
  pccProject?: IRequestHintProject
  segmentId?: string
  organization?: string
}

export type RequestQuestionKind = 'did_you_mean' | 'lf_conflict' | 'lf_confirm' | 'pick_project'

export interface IRequestQuestionOption {
  id: string
  label: string
  hints?: IRequestHints
  handOff?: boolean
  suggested?: boolean
}

export interface IRequestQuestion {
  kind: RequestQuestionKind
  text: string
  options: IRequestQuestionOption[]
  messageTs?: string | null
  answeredBy?: string | null
  answeredAt?: string | null
  answer?: string | null
}

export type ProjectCatalogReviewDecision = 'approve' | 'reject' | 'done'

export interface IDbProjectCatalogReviewResolution {
  decision: ProjectCatalogReviewDecision
  reviewedBy: string
  decisionReason?: string | null
  decisionNote?: string | null
}

export const PROJECT_CATALOG_HUMAN_PROVENANCES = [
  'github-discussion',
  'slack-tag',
  'slack-bot',
] as const
export const PROJECT_CATALOG_BULK_PROVENANCES = ['lf-criticality-score'] as const

export const PROJECT_CATALOG_PROVENANCES = [
  ...PROJECT_CATALOG_HUMAN_PROVENANCES,
  ...PROJECT_CATALOG_BULK_PROVENANCES,
] as const

export type ProjectCatalogProvenance = (typeof PROJECT_CATALOG_PROVENANCES)[number]
export type ProjectCatalogHumanProvenance = (typeof PROJECT_CATALOG_HUMAN_PROVENANCES)[number]

export function isHumanProjectCatalogProvenance(
  value: ProjectCatalogProvenance,
): value is ProjectCatalogHumanProvenance {
  return (PROJECT_CATALOG_HUMAN_PROVENANCES as readonly string[]).includes(value)
}

export function isGithubDiscussionProvenance(
  value: ProjectCatalogProvenance | null,
): value is 'github-discussion' {
  return value === 'github-discussion'
}

export function isSlackBotProvenance(value: ProjectCatalogProvenance | null): value is 'slack-bot' {
  return value === 'slack-bot'
}

const SLACK_PERMALINK_PATTERN = /^https:\/\/[^/]+\.slack\.com\/archives\//

export function isSlackPermalink(url: string): boolean {
  return SLACK_PERMALINK_PATTERN.test(url)
}

export function isReviewAlertProvenance(
  value: ProjectCatalogProvenance | null,
): value is 'github-discussion' | 'slack-bot' {
  return isGithubDiscussionProvenance(value) || isSlackBotProvenance(value)
}

export interface IDbProjectCatalog {
  id: string
  projectSlug: string
  repoName: string
  repoUrl: string
  source: string | null
  sourceUrl: string | null
  provenance: ProjectCatalogProvenance | null
  action: ProjectCatalogAction
  lfCriticalityScore: number | null
  evaluationResult: string | null
  evaluationReason: string | null
  evaluationAttempts: number
  evaluatedAt: string | null
  onboardedAt: string | null
  onboardingError: string | null
  skipReason: string | null
  requestText: string | null
  projectName: string | null
  classificationKind: ProjectCatalogClassificationKind | null
  proposedAction: ProjectCatalogProposedAction | null
  pccProjectId: string | null
  classifiedAt: string | null
  reviewedAt: string | null
  reviewedBy: string | null
  reviewMessageTs: string | null
  reviewReason: string | null
  matchedProjectName: string | null
  matchedSegmentId: string | null
  decisionReason: string | null
  decisionNote: string | null
  requestedBy: string | null
  requestQuestion: IRequestQuestion | null
  requestHints: IRequestHints | null
  syncedAt: string | null
  createdAt: string | null
  updatedAt: string | null
}

type ProjectCatalogWritable = Pick<
  IDbProjectCatalog,
  | 'projectSlug'
  | 'repoName'
  | 'repoUrl'
  | 'source'
  | 'action'
  | 'lfCriticalityScore'
  | 'evaluationResult'
  | 'evaluationReason'
  | 'onboardingError'
  | 'skipReason'
  | 'requestText'
  | 'projectName'
>

export type IDbProjectCatalogRef = Pick<IDbProjectCatalog, 'id' | 'repoUrl'>

export interface IDbProjectCatalogReopened extends IDbProjectCatalogRef {
  previousSourceUrl: string | null
  previousDecisionReason: string | null
}

export interface IProjectCatalogReopenRequest {
  repoUrl: string
  sourceUrl: string
  requestText: string
  projectName: string | null
  requestedBy: string | null
  organization: string | null
}

export type IDbProjectCatalogCreate = Omit<
  ProjectCatalogWritable,
  | 'source'
  | 'action'
  | 'lfCriticalityScore'
  | 'evaluationResult'
  | 'evaluationReason'
  | 'onboardingError'
  | 'skipReason'
  | 'requestText'
  | 'projectName'
> & {
  source?: string | null
  sourceUrl?: string | null
  provenance?: ProjectCatalogProvenance | null
  action?: ProjectCatalogAction
  lfCriticalityScore?: number
  evaluationResult?: string | null
  evaluationReason?: string | null
  onboardingError?: string | null
  skipReason?: string | null
  requestText?: string | null
  projectName?: string | null
  requestedBy?: string | null
  requestHints?: IRequestHints | null
}

export type IDbProjectCatalogUpdate = Partial<ProjectCatalogWritable> & {
  syncedAt?: string | null
  evaluatedAt?: string | null
  onboardedAt?: string | null
  matchedSegmentId?: string | null
}

export interface IDbProjectCatalogClassification {
  projectName: string | null
  classificationKind: ProjectCatalogClassificationKind
  proposedAction: ProjectCatalogProposedAction | null
  pccProjectId: string | null
  matchedProjectName: string | null
  matchedSegmentId: string | null
  action: ProjectCatalogRoutedAction
  skipReason: string | null
  reviewReason: string | null
  requestQuestion?: IRequestQuestion | null
}

export interface IDbProjectCatalogAnswer {
  answer: string
  answeredBy: string
  hints: IRequestHints | null
  action: Extract<ProjectCatalogAction, 'evaluate' | 'review'>
  reviewReason: string | null
  proposedAction: Extract<ProjectCatalogProposedAction, 'human_review'> | null
}

// Shared contract between the projects evaluation worker and the /project-evaluation
// public API so the two sides can't drift silently.
export interface IProjectEvaluationRequest {
  id: string
  repoUrl: string
  repoName: string
  projectSlug: string
  lfCriticalityScore: number | null
  source: string | null
}

export type ProjectEvaluationOutcome = Extract<ProjectCatalogAction, 'onboard' | 'skip' | 'unsure'>

export interface IProjectEvaluationMetrics {
  model: string
  inputTokens: number
  outputTokens: number
  seconds: number
}

export interface IProjectEvaluationResponse {
  outcome: ProjectEvaluationOutcome
  evaluationResult: string
  evaluationReason: string | null
  metrics: IProjectEvaluationMetrics | null
}
