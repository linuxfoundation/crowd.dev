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

// Returned by evaluateAndUpdateProject so the workflow can aggregate cost/token usage.
// null when the API was never called (already evaluated, or raced out on update).
export interface IEvaluationActivityResult {
  outcome: EvaluationOutcome
  model: string | null
  inputTokens: number | null
  outputTokens: number | null
  costUsd: number | null
  seconds: number | null
}
