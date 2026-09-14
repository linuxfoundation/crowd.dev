export const PIPELINE_RUN_STAGES = ['discovery', 'evaluation', 'onboarding'] as const

export type PipelineRunStage = (typeof PIPELINE_RUN_STAGES)[number]

export const PIPELINE_RUN_STATUSES = ['running', 'completed', 'failed'] as const

export type PipelineRunStatus = (typeof PIPELINE_RUN_STATUSES)[number]

export interface IPipelineRunEvaluatorModelUsage {
  calls: number
  inputTokens: number
  outputTokens: number
  costUsd: number | null
}

export interface IDbPipelineRun {
  id: string
  stage: PipelineRunStage
  status: PipelineRunStatus
  workflowId: string | null
  temporalRunId: string | null
  startedAt: string
  finishedAt: string | null
  elapsedSeconds: number | null
  totalCandidates: number | null
  succeeded: number | null
  failed: number | null
  skipped: number | null
  skippedPreCheck: number | null
  errorMessage: string | null
  details: Record<string, unknown> | null
  evaluatorCalls: number | null
  evaluatorInputTokens: number | null
  evaluatorOutputTokens: number | null
  evaluatorCostUsd: number | null
  evaluatorSeconds: number | null
  evaluatorModels: Record<string, IPipelineRunEvaluatorModelUsage> | null
  createdAt: string
  updatedAt: string
}

export interface IPipelineRunStart {
  stage: PipelineRunStage
  workflowId?: string | null
  temporalRunId?: string | null
}

export interface IPipelineRunEvaluatorUsage {
  calls: number
  inputTokens: number
  outputTokens: number
  costUsd: number | null
  seconds: number | null
  models: Record<string, IPipelineRunEvaluatorModelUsage>
}

export interface IPipelineRunFinish {
  status: PipelineRunStatus
  totalCandidates?: number | null
  succeeded?: number | null
  failed?: number | null
  skipped?: number | null
  skippedPreCheck?: number | null
  errorMessage?: string | null
  details?: Record<string, unknown> | null
  evaluator?: IPipelineRunEvaluatorUsage | null
}

export interface IPipelineRunFilter {
  stage?: PipelineRunStage
  from?: string
  to?: string
  limit?: number
  offset?: number
}
