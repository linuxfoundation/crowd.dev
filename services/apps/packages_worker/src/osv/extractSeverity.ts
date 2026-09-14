import { computeV3Score } from './cvssScoring'
import { CvssSource, OsvRecord } from './types'

const QUALITATIVE_TO_CVSS: Record<string, number> = {
  CRITICAL: 9.5,
  HIGH: 7.5,
  MEDIUM: 5.0,
  LOW: 3.0,
}

export interface SeverityResult {
  severity: string | null
  cvss: number | null
  cvssSource: CvssSource | null
}

// Plan §4 / decision #6: MAL- ids are malicious-package reports with no CVSS
// vector; we mark them with a dedicated cvss_source so deriveCriticalFlag can
// flip the package flag via id-prefix match without conflating with CVSS≥7.0.
function isMalicious(id: string): boolean {
  return id.startsWith('MAL-')
}

// extractSeverity is pure. The intended order per ADR-0001 §CVSS scoring
// strategy is V4 → V3 →
// qualitative tag from database_specific.severity, but v4 numeric scoring is
// deferred (see cvssScoring.ts), so v1 skips V4 entirely: V3 first, then the
// qualitative tag. V4-only records fall through to the qualitative fallback
// and record cvssSource as 'osv_qualitative_fallback'.
export function extractSeverity(record: OsvRecord): SeverityResult {
  if (isMalicious(record.id)) {
    return { severity: null, cvss: null, cvssSource: 'osv_malicious_package' }
  }

  const qualitativeRaw = record.database_specific?.severity ?? null
  const qualitative = qualitativeRaw ? qualitativeRaw.toUpperCase() : null

  const severityList = record.severity ?? []
  const v3 = severityList.find((s) => s.type === 'CVSS_V3')

  // V4 vector parsing is not implemented for v1. If V3 is present alongside V4
  // (the common case per the spike), we score V3. If V4 is the only vector,
  // we drop through to qualitative below.
  if (v3) {
    const score = computeV3Score(v3.score)
    if (score !== null) {
      return { severity: qualitative, cvss: score, cvssSource: 'osv_cvss_v3' }
    }
  }

  if (qualitative && qualitative in QUALITATIVE_TO_CVSS) {
    return {
      severity: qualitative,
      cvss: QUALITATIVE_TO_CVSS[qualitative],
      cvssSource: 'osv_qualitative_fallback',
    }
  }

  return { severity: null, cvss: null, cvssSource: null }
}
