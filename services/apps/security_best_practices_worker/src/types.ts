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
  applicability: string[]
  description: string
  result: string
  message: string
  steps: string[]
  'steps-executed': number
  start: string
  end?: string
  recommendation?: string
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
  lastUsed: Date
  isRateLimited: boolean
}
