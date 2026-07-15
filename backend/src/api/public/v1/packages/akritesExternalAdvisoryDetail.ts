import type { AkritesExternalAdvisoryRow } from '@crowd/data-access-layer'

export type AkritesExternalAdvisorySeverity = 'critical' | 'high' | 'moderate' | 'low' | null
export type AkritesExternalAdvisoryResolution = 'open' | 'patched' | null

export interface AkritesExternalAdvisory {
  osvId: string
  severity: AkritesExternalAdvisorySeverity
  resolution: AkritesExternalAdvisoryResolution
  isCritical: boolean | null
}

export interface AkritesExternalAdvisoryDetail {
  purl: string
  advisories: AkritesExternalAdvisory[]
}

export interface AdvisoryDetailBulkEntry {
  requestedPurl: string
  found: boolean
  advisories: AkritesExternalAdvisoryDetail | null
}

// Severity values the contract's enum accepts. The DB stores severity uppercase and
// the query lowercases it; anything outside this set (or null) maps to null rather
// than being echoed verbatim, so the response never violates the published enum.
const SEVERITY_SET = new Set<string>(['critical', 'high', 'moderate', 'low'])

function toAkritesSeverity(severity: string | null): AkritesExternalAdvisorySeverity {
  if (severity === null || !SEVERITY_SET.has(severity)) return null
  return severity as AkritesExternalAdvisorySeverity
}

// Builds the AdvisoryDetail for a single purl from its DAL rows. Rows carry a null
// osvId sentinel for a found-but-advisory-less package (see AkritesExternalAdvisoryRow) —
// those are dropped here so `advisories` is an empty array, not a list with a null entry.
export function toAkritesExternalAdvisoryDetail(
  purl: string,
  rows: AkritesExternalAdvisoryRow[],
): AkritesExternalAdvisoryDetail {
  const advisories: AkritesExternalAdvisory[] = rows
    .filter((r): r is AkritesExternalAdvisoryRow & { osvId: string } => r.osvId !== null)
    .map((r) => ({
      osvId: r.osvId,
      severity: toAkritesSeverity(r.severity),
      resolution: r.resolution,
      isCritical: r.isCritical,
    }))

  return { purl, advisories }
}
