export interface ISecurityInsightsPrivateerResult {
  'evaluation-suites': ISecurityInsightsPrivateerEvaluationSuite[]
}

export interface ISecurityInsightsPrivateerEvaluationSuite {
  name: string
  'catalog-id': string
  'start-time': string
  'end-time': string
  result: string
  'corrupted-state': boolean
  'control-evaluations': {
    result: string
    evaluations: ISecurityInsightsPrivateerResultControlEvaluations[]
  }
}

export interface ISecurityInsightsPrivateerResultControlEvaluations {
  name: string
  control: { 'reference-id': string; 'entry-id': string }
  result: string
  message: string
  'assessment-logs': ISecurityInsightsPrivateerResultAssessment[]
}

export interface ISecurityInsightsPrivateerResultAssessment {
  requirement: { 'reference-id': string; 'entry-id': string }
  plan?: { 'reference-id': string; 'entry-id': string }
  applicability: string[]
  description: string
  result: string
  message: string
  steps: string[]
  'steps-executed': number
  start: string
  end?: string
  recommendation?: string
  'confidence-level'?: string
}

export interface IUpsertOSPSBaselineSecurityInsightsParams {
  insightsProjectId: string
  insightsProjectSlug: string
  repoUrl: string
  token: string
}

export interface ITriggerSecurityInsightsCheckForReposParams {
  failedRepoUrls?: string[]
}

export interface ITokenInfo {
  token: string
  inUse: boolean
  // Date at initialization time; becomes an ISO string after JSON round-trip through Redis
  // and Temporal payloads, so callers must wrap in `new Date()` before comparing.
  lastUsed: Date | string
  isRateLimited: boolean
  rateLimitedAt?: string // ISO timestamp; used to auto-reset after 1 hour
  isInvalid?: boolean // 401 auth failure; recover by rotating the PAT out of CROWD_GITHUB_PERSONAL_ACCESS_TOKENS
}
