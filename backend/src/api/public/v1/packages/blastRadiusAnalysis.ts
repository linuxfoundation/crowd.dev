import type {
  AnalysisDetailRow,
  VerdictResultRow,
} from '@crowd/data-access-layer/src/packages/blastRadius'

import type { BlastRadiusJobEcosystem, BlastRadiusJobStatus } from './blastRadius'

export type BlastRadiusResultConfidence = 'high' | 'medium' | 'low'

export interface BlastRadiusResultItem {
  dependent: string
  affected: boolean
  confidence: BlastRadiusResultConfidence
  evidence: string | null
  downloadsLast30Days: string | null
}

export interface BlastRadiusAnalysisSummary {
  totalDependentsInRange: number
  dependentsExcludedUpfront: number
  dependentsAnalyzed: number
  dependentsAffected: number
  affectedPercentage: number | null
  affectedDependents: string[]
}

export interface BlastRadiusAnalysis {
  analysisId: string
  status: BlastRadiusJobStatus
  advisoryId: string
  package: string | null
  ecosystem: BlastRadiusJobEcosystem
  submittedAt: string | null
  completedAt: string | null
  errorMessage: string | null
  summary: BlastRadiusAnalysisSummary | null
  results: BlastRadiusResultItem[] | null
}

// Crosswalk from the DB's NUMERIC(3,2) 0-1 confidence score to the contract's
// enum — matches the PoC methodology's confidence bands (report.py).
function toResultConfidence(confidence: number): BlastRadiusResultConfidence {
  if (confidence >= 0.8) return 'high'
  if (confidence >= 0.4) return 'medium'
  return 'low'
}

// npm purls use a literal (unencoded) `@` for the scope, matching the contract's
// example response bodies — these are JSON fields, not URL query params, so the
// %40-encoding normalizePurl applies elsewhere in this file group does not apply here.
function toPurl(name: string): string {
  return `pkg:npm/${name}`
}

function flattenEvidence(evidence: Record<string, unknown>[] | null): string | null {
  if (!evidence || evidence.length === 0) return null
  return evidence
    .map((e) => {
      const file = e.file ? String(e.file) : null
      const line = e.line !== undefined && e.line !== null ? String(e.line) : null
      const snippet = e.snippet ? String(e.snippet) : null
      const location = [file, line].filter(Boolean).join(':')
      return [location, snippet].filter(Boolean).join(' — ')
    })
    .filter(Boolean)
    .join('\n')
}

function toResultItem(row: VerdictResultRow): BlastRadiusResultItem {
  return {
    dependent: toPurl(row.name),
    affected: row.reachable_verdict === 'affected',
    confidence: toResultConfidence(row.confidence),
    evidence: flattenEvidence(row.evidence),
    downloadsLast30Days: row.downloads !== null ? String(row.downloads) : null,
  }
}

// Population-level summary, following the PoC's report.py ground truth:
// totalDependentsInRange = candidates_considered (post phase-1-filter population),
// dependentsAnalyzed = number of verdicts produced, dependentsExcludedUpfront =
// the difference (range-excluded before reachability ran), dependentsAffected =
// count with an 'affected' verdict, affectedPercentage rounded to 1 decimal
// (null when nothing was analyzed, to avoid a misleading 0%).
function toSummary(
  candidatesConsidered: number | null,
  results: BlastRadiusResultItem[],
): BlastRadiusAnalysisSummary {
  const totalDependentsInRange = candidatesConsidered ?? results.length
  const dependentsAnalyzed = results.length
  const affected = results.filter((r) => r.affected)

  return {
    totalDependentsInRange,
    dependentsExcludedUpfront: Math.max(totalDependentsInRange - dependentsAnalyzed, 0),
    dependentsAnalyzed,
    dependentsAffected: affected.length,
    affectedPercentage:
      dependentsAnalyzed > 0
        ? Math.round((affected.length / dependentsAnalyzed) * 1000) / 10
        : null,
    affectedDependents: affected.map((r) => r.dependent),
  }
}

export function toBlastRadiusAnalysis(
  analysis: AnalysisDetailRow,
  verdictRows: VerdictResultRow[],
): BlastRadiusAnalysis {
  const status = analysis.status as BlastRadiusJobStatus
  const done = status === 'done'

  const results = done ? verdictRows.map(toResultItem) : null

  return {
    analysisId: analysis.id,
    status,
    advisoryId: analysis.advisory_osv_id,
    package: analysis.package_name,
    ecosystem: analysis.ecosystem as BlastRadiusJobEcosystem,
    submittedAt: analysis.started_at,
    completedAt: analysis.completed_at,
    errorMessage: analysis.error,
    summary: done && results ? toSummary(analysis.candidates_considered, results) : null,
    results,
  }
}
