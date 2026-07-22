import type {
  AnalysisDetailRow,
  VerdictResultRow,
} from '@crowd/data-access-layer/src/packages/blastRadius'

import type { BlastRadiusJobEcosystem, BlastRadiusJobStatus } from './blastRadius'

export type BlastRadiusResultConfidence = 'high' | 'medium' | 'low'

export type BlastRadiusVerdict = 'affected' | 'not_affected' | 'unclear'

export interface BlastRadiusResultItem {
  dependent: string
  affected: boolean
  verdict: BlastRadiusVerdict
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

// Scoped npm names (@scope/name) need their leading @ percent-encoded to %40 —
// matching both purl.ts's normalizePurl and the contract's own example response
// bodies (e.g. pkg:npm/%40angular/core in openapi.yaml).
function toPurl(name: string): string {
  return `pkg:npm/${name.replace(/^@/, '%40')}`
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

// reachable_verdict is 'affected' | 'not_affected' | 'unclear' (see VERDICT_SCHEMA in
// agent/prompts.ts) — 'unclear' covers both a genuine ambiguous read and a persistent
// agent failure (upsertErrorVerdict in reachability.ts). affected=false alone can't
// distinguish "confirmed not affected" from "we don't know" — expose the raw verdict
// too so consumers who care can tell the difference.
function toVerdict(reachableVerdict: string): BlastRadiusVerdict {
  if (reachableVerdict === 'affected' || reachableVerdict === 'not_affected') {
    return reachableVerdict
  }
  return 'unclear'
}

function toResultItem(row: VerdictResultRow): BlastRadiusResultItem {
  const verdict = toVerdict(row.reachable_verdict)
  return {
    dependent: toPurl(row.name),
    affected: verdict === 'affected',
    verdict,
    confidence: toResultConfidence(row.confidence),
    evidence: flattenEvidence(row.evidence),
    downloadsLast30Days: row.downloads !== null ? String(row.downloads) : null,
  }
}

// Population-level summary. dependentsExcludedUpfront comes from the
// blast_radius_dependents rows actually marked excluded_by_range=true — NOT from
// candidates_considered, which is stage 2's phase-1 population and also counts
// candidates the topN walk never reached (so candidatesConsidered - analyzed
// overstates range exclusions). dependentsAnalyzed = number of verdicts produced,
// dependentsAffected = count with an 'affected' verdict, affectedPercentage rounded
// to 1 decimal — computed over conclusive verdicts only (affected/not_affected), so
// persistent agent/tarball failures ('unclear') don't drag it toward a misleading 0%.
// null when nothing conclusive was analyzed.
function toSummary(
  dependentsExcludedUpfront: number,
  results: BlastRadiusResultItem[],
): BlastRadiusAnalysisSummary {
  const dependentsAnalyzed = results.length
  const affected = results.filter((r) => r.affected)
  const conclusive = results.filter((r) => r.verdict !== 'unclear')

  return {
    totalDependentsInRange: dependentsAnalyzed + dependentsExcludedUpfront,
    dependentsExcludedUpfront,
    dependentsAnalyzed,
    dependentsAffected: affected.length,
    affectedPercentage:
      conclusive.length > 0
        ? Math.round((affected.length / conclusive.length) * 1000) / 10
        : null,
    affectedDependents: affected.map((r) => r.dependent),
  }
}

export function toBlastRadiusAnalysis(
  analysis: AnalysisDetailRow,
  verdictRows: VerdictResultRow[],
  dependentsExcludedByRangeCount: number,
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
    summary: done && results ? toSummary(dependentsExcludedByRangeCount, results) : null,
    results,
  }
}
