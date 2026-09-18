import type { DocDiscoveryConfidence, DocDiscoveryMethod } from '../project-doc-discoveries/types'
import type { DocReadinessRunScope } from '../project-doc-readiness-runs/types'

export const DOC_READINESS_CHECK_STATUSES = ['pass', 'warn', 'fail', 'skip', 'error'] as const

export type DocReadinessCheckStatus = (typeof DOC_READINESS_CHECK_STATUSES)[number]

export const DOCS_READINESS_SWEEP_MODES = ['full', 'incremental'] as const

export type DocsReadinessSweepMode = (typeof DOCS_READINESS_SWEEP_MODES)[number]

export interface IDbProjectDocReadiness {
  id: string
  projectId: string
  projectSlug: string
  projectName: string
  docsUrl: string | null
  discoveryMethod: DocDiscoveryMethod | null
  confidence: DocDiscoveryConfidence | null
  isOverride: boolean
  overallScore: number | null
  overallGrade: string | null
  categoryScores: Record<string, number> | null
  runDate: string
  runId: string | null
  durationMs: number | null
  ok: boolean
  error: string | null
  createdAt: string
  updatedAt: string
}

export interface IProjectDocReadinessUpsert {
  projectId: string
  projectSlug: string
  projectName: string
  docsUrl: string | null
  discoveryMethod: DocDiscoveryMethod | null
  confidence: DocDiscoveryConfidence | null
  isOverride: boolean
  overallScore: number | null
  overallGrade: string | null
  categoryScores: Record<string, number> | null
  runId: string | null
  durationMs: number | null
  ok: boolean
  error: string | null
  /** Defaults to the current date. */
  runDate?: string
}

export interface IDbProjectDocReadinessCheck {
  projectId: string
  checkId: string
  category: string
  status: DocReadinessCheckStatus
  message: string | null
  details: string | null
  durationMs: number | null
  scoredAt: string
  createdAt: string
  updatedAt: string
}

export interface IProjectDocReadinessCheckInsert {
  checkId: string
  category: string
  status: DocReadinessCheckStatus
  message: string | null
  details: string | null
  durationMs: number | null
}

export interface IProjectForDocsReadiness {
  id: string
  slug: string
  name: string
}

export interface IFindProjectsForDocsReadiness {
  mode: DocsReadinessSweepMode
  scope: DocReadinessRunScope
  afterId?: string | null
  limit: number
}
