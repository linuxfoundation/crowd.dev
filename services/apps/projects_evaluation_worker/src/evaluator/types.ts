import {
  IProjectEvaluationMetrics,
  IProjectEvaluationRequest,
  IProjectEvaluationResponse,
  ProjectEvaluationOutcome,
} from '@crowd/data-access-layer/src/project-catalog/types'

export type IEvaluationInput = IProjectEvaluationRequest
export type EvaluationOutcome = ProjectEvaluationOutcome
export type IEvaluationMetrics = IProjectEvaluationMetrics
export type IEvaluationResult = IProjectEvaluationResponse
