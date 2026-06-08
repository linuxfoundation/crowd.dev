import { ProjectCatalogAction } from '@crowd/data-access-layer/src/project-catalog/types'

export interface IEvaluationInput {
  id: string
  repoUrl: string
  repoName: string
  projectSlug: string
  lfCriticalityScore: number | null
  source: string | null
}

// Evaluation can only resolve to 'onboard', 'skip', or 'unsure' — never back to 'evaluate' or 'auto'.
export type EvaluationOutcome = Extract<ProjectCatalogAction, 'onboard' | 'skip' | 'unsure'>

export interface IEvaluationResult {
  outcome: EvaluationOutcome
  evaluationResult: string
  evaluationReason: string | null
}
