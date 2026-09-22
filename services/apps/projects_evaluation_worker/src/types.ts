import { IDbProjectCatalog } from '@crowd/data-access-layer/src/project-catalog/types'

import { EvaluationOutcome } from './evaluator/types'

export interface IPriorityConfig {
  /** Maximum number of projects in the 'evaluate' queue at any time. */
  evaluateLimit: number
  /**
   * Ordered list of source names by descending priority.
   * Sources not in this list rank below all listed ones.
   */
  sourcePriority: string[]
}

export interface IEvaluateProjectsInput {
  batchSize?: number
  priorityConfig?: IPriorityConfig
}

// Returned by precheckPendingProjects; breakdown keys are the skip reasons from
// PRECHECK_SKIP_REASONS, values are the count of projects skipped for that reason.
export interface IPrecheckResult {
  remaining: IDbProjectCatalog[]
  skippedPreCheck: number
  breakdown: Record<string, number>
}

// Returned by evaluateAndUpdateProject so the workflow can aggregate cost/token usage.
// null when the API was never called (project already evaluated).
export interface IEvaluationActivityResult {
  applied: boolean
  outcome: EvaluationOutcome
  model: string | null
  inputTokens: number | null
  outputTokens: number | null
  costUsd: number | null
  seconds: number | null
}
