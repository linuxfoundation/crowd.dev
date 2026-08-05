import { compareVersion } from '../../osv/versionCompare'
import { OsvAffectedPackage } from '../clients/osvClient'

import { compareNuGetVersion } from './nuget/nugetVersionCompare'

// Shared range type for Maven and NuGet (ecosystems with ECOSYSTEM-typed OSV ranges).
// Parameterized by ecosystem to avoid duplication; see mavenVersions.ts for wrappers.
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

// NuGet accepts a 4th numeric component (Major.Minor.Patch.Revision), which node-semver
// rejects outright — see nugetVersionCompare.ts for why 'nuget' can't share compareVersion.
function compareOrNull(ecosystem: string, a: string, b: string): number | null {
  if (ecosystem === 'nuget') return compareNuGetVersion(a, b)
  return compareVersion(ecosystem, a, b)
}

// Unparseable bound → treated as NOT in range (unlike the constraint helpers' over-inclusive
// stance — this only decides the vulnerable-version set, not dependent reachability).
function isInRange(ecosystem: string, version: string, range: EcosystemRange): boolean {
  // OSV defines introduced: "0" as "vulnerable from the beginning" — not a real version
  // to parse/compare (see osv/deriveCriticalFlag.ts's identical special case).
  if (range.introduced && range.introduced !== '0') {
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
