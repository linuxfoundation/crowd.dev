export type EvaluationStatus = 'pending' | 'evaluating' | 'evaluated' | 'failed'

export interface IDbEvaluatedProject {
  id: string
  projectCatalogId: string
  evaluationStatus: EvaluationStatus
  evaluationScore: number | null
  evaluation: Record<string, unknown> | null
  evaluationReason: string | null
  evaluatedAt: string | null
  starsCount: number | null
  forksCount: number | null
  commitsCount: number | null
  pullRequestsCount: number | null
  issuesCount: number | null
  onboarded: boolean
  onboardedAt: string | null
  createdAt: string | null
  updatedAt: string | null
}

type EvaluatedProjectWritable = Pick<
  IDbEvaluatedProject,
  | 'projectCatalogId'
  | 'evaluationStatus'
  | 'evaluationScore'
  | 'evaluation'
  | 'evaluationReason'
  | 'evaluatedAt'
  | 'starsCount'
  | 'forksCount'
  | 'commitsCount'
  | 'pullRequestsCount'
  | 'issuesCount'
  | 'onboarded'
  | 'onboardedAt'
>

export type IDbEvaluatedProjectCreate = Omit<EvaluatedProjectWritable, 'projectCatalogId'> & {
  projectCatalogId: string
} & {
  evaluationStatus?: EvaluationStatus
  evaluationScore?: number
  evaluation?: Record<string, unknown>
  evaluationReason?: string
  evaluatedAt?: string
  starsCount?: number
  forksCount?: number
  commitsCount?: number
  pullRequestsCount?: number
  issuesCount?: number
  onboarded?: boolean
  onboardedAt?: string
}

export type IDbEvaluatedProjectUpdate = Partial<{
  evaluationStatus: EvaluationStatus
  evaluationScore: number
  evaluation: Record<string, unknown>
  evaluationReason: string
  evaluatedAt: string
  starsCount: number
  forksCount: number
  commitsCount: number
  pullRequestsCount: number
  issuesCount: number
  onboarded: boolean
  onboardedAt: string
}>
