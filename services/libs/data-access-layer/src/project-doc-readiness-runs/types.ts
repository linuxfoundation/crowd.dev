export const DOC_READINESS_RUN_TRIGGERS = [
  'scheduled-full',
  'scheduled-incremental',
  'on-demand',
] as const

export type DocReadinessRunTrigger = (typeof DOC_READINESS_RUN_TRIGGERS)[number]

export const DOC_READINESS_RUN_SCOPES = ['lf', 'all'] as const

export type DocReadinessRunScope = (typeof DOC_READINESS_RUN_SCOPES)[number]

export const DOC_READINESS_RUN_STATUSES = ['running', 'completed', 'failed'] as const

export type DocReadinessRunStatus = (typeof DOC_READINESS_RUN_STATUSES)[number]

export type DocReadinessRunTerminalStatus = Exclude<DocReadinessRunStatus, 'running'>

export interface IDbDocReadinessRun {
  id: string
  trigger: DocReadinessRunTrigger
  scope: DocReadinessRunScope
  status: DocReadinessRunStatus
  workflowId: string | null
  temporalRunId: string | null
  startedAt: string
  finishedAt: string | null
  totalProjects: number | null
  discovered: number | null
  scored: number | null
  failed: number | null
  errorMessage: string | null
  createdAt: string
  updatedAt: string
}

export interface IDocReadinessRunStart {
  trigger: DocReadinessRunTrigger
  scope: DocReadinessRunScope
  workflowId?: string | null
  temporalRunId?: string | null
}

export interface IDocReadinessRunCounters {
  totalProjects?: number | null
  discovered?: number | null
  scored?: number | null
  failed?: number | null
}

export interface IDocReadinessRunFinish extends IDocReadinessRunCounters {
  status: DocReadinessRunTerminalStatus
  errorMessage?: string | null
}
