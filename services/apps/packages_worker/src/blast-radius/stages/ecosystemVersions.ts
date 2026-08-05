import { compareVersion } from '../../osv/versionCompare'
import { OsvAffectedPackage } from '../clients/osvClient'

// Shared by any ecosystem whose OSV advisories use ECOSYSTEM-typed ranges (ordered via
// compareVersion(ecosystem, …) rather than node-semver) instead of SEMVER-typed ranges —
// currently Maven and NuGet. Parameterized by ecosystem so the two don't duplicate this
// logic; see mavenVersions.ts for the ecosystem-bound wrappers Maven's stage files import.
export interface EcosystemRange {
  introduced: string | null
  fixed: string | null
  lastAffected: string | null
}

export function ecosystemRangeEvents(entry: OsvAffectedPackage): EcosystemRange[] {
  const events: EcosystemRange[] = []

  if (entry.ranges) {
    for (const range of entry.ranges) {
      if (range.type !== 'ECOSYSTEM' || !range.events) continue

      let introduced: string | null = null

      for (const event of range.events) {
        if (event.introduced) introduced = event.introduced

        if (event.fixed) {
          events.push({ introduced, fixed: event.fixed, lastAffected: null })
          introduced = null
        } else if (event.last_affected) {
          events.push({ introduced, fixed: null, lastAffected: event.last_affected })
          introduced = null
        }
      }

      if (introduced !== null) {
        events.push({ introduced, fixed: null, lastAffected: null })
      }
    }
  }

  // Fallback: if no ECOSYSTEM ranges, use the explicit version list as exact
  // vulnerable versions.
  if (events.length === 0 && (entry.versions ?? []).length > 0) {
    for (const v of entry.versions ?? []) {
      events.push({ introduced: v, fixed: null, lastAffected: v })
    }
  }

  return events
}

function compareOrNull(ecosystem: string, a: string, b: string): number | null {
  return compareVersion(ecosystem, a, b)
}

// Unparseable bound → treated as NOT in range (unlike the constraint helpers' over-inclusive
// stance — this only decides the vulnerable-version set, not dependent reachability).
function isInRange(ecosystem: string, version: string, range: EcosystemRange): boolean {
  if (range.introduced) {
    const c = compareOrNull(ecosystem, version, range.introduced)
    if (c === null || c < 0) return false
  }
  if (range.fixed) {
    const c = compareOrNull(ecosystem, version, range.fixed)
    if (c === null || c >= 0) return false
  }
  if (range.lastAffected) {
    const c = compareOrNull(ecosystem, version, range.lastAffected)
    if (c === null || c > 0) return false
  }
  return true
}

export function versionsInRanges(
  ecosystem: string,
  versions: string[],
  ranges: EcosystemRange[],
): string[] {
  const result: string[] = []
  for (const v of versions) {
    for (const range of ranges) {
      if (isInRange(ecosystem, v, range)) {
        result.push(v)
        break
      }
    }
  }
  return result
}

export function highestVersion(ecosystem: string, versions: string[]): string | null {
  let best: string | null = null
  for (const v of versions) {
    if (best === null) {
      best = v
      continue
    }
    const c = compareOrNull(ecosystem, v, best)
    if (c !== null && c > 0) best = v
  }
  return best
}
