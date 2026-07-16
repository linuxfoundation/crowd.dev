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

// Maps the DB's (lowercased) severity vocabulary to the contract's enum. The DB
// normalizes the middle band to MEDIUM (initial_schema.sql; osv/extractSeverity.ts),
// whereas the Akrites contract calls that same level `moderate` — hence the explicit
// medium → moderate crosswalk. Anything unrecognized (or null) maps to null so the
// response never violates the published enum.
const SEVERITY_CROSSWALK: Record<string, AkritesExternalAdvisorySeverity> = {
  critical: 'critical',
  high: 'high',
  medium: 'moderate',
  moderate: 'moderate',
  low: 'low',
}

function toAkritesSeverity(severity: string | null): AkritesExternalAdvisorySeverity {
  if (severity === null) return null
  return SEVERITY_CROSSWALK[severity] ?? null
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
