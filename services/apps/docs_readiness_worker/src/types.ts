import type {
  DocDiscoveryConfidence,
  DocDiscoveryMethod,
  DocReadinessRunScope,
  DocsReadinessSweepMode,
  IDocReadinessRunCounters,
} from '@crowd/data-access-layer'

export const DOCS_READINESS_TASK_QUEUE = 'docs-readiness'

export const DEFAULT_SWEEP_CONCURRENCY = 10

export interface IRunDocsReadinessSweepArgs {
  mode: DocsReadinessSweepMode
  scope: DocReadinessRunScope
  concurrency?: number
  // Carried across continueAsNew boundaries; never set by the caller.
  runId?: string
  afterId?: string
  counters?: IDocReadinessRunCounters
}

export interface IProcessProjectDocsReadinessArgs {
  projectId: string
  // Set when started as a child of a sweep; absent for on-demand runs.
  runId?: string
}

export interface IResolvedDocsUrl {
  docsUrl: string | null
  discoveryMethod: DocDiscoveryMethod | null
  confidence: DocDiscoveryConfidence | null
  isOverride: boolean
}
