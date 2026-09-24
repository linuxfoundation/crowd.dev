export const PROJECT_CATALOG_ACTIONS = [
  'auto',
  'evaluate',
  'onboard',
  'onboarded',
  'skip',
  'unsure',
  'error',
] as const

export type ProjectCatalogAction = (typeof PROJECT_CATALOG_ACTIONS)[number]

export type ProjectCatalogActionCounts = Record<ProjectCatalogAction, number>

export const PROJECT_CATALOG_HUMAN_PROVENANCES = ['github-discussion', 'slack-tag'] as const
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
  evaluatedAt: string | null
  onboardedAt: string | null
  onboardingError: string | null
  skipReason: string | null
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
>

export type IDbProjectCatalogCreate = Omit<
  ProjectCatalogWritable,
  | 'source'
  | 'action'
  | 'lfCriticalityScore'
  | 'evaluationResult'
  | 'evaluationReason'
  | 'onboardingError'
  | 'skipReason'
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
}

export type IDbProjectCatalogUpdate = Partial<ProjectCatalogWritable> & {
  syncedAt?: string | null
  evaluatedAt?: string | null
  onboardedAt?: string | null
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
