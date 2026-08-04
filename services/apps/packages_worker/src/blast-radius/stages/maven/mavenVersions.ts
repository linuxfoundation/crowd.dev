import { compareVersion } from '../../../osv/versionCompare'
import { OsvAffectedPackage } from '../../clients/osvClient'

// Maven-specific counterpart to semverRange.ts — OSV Maven advisories use ECOSYSTEM-typed
// ranges, ordered via compareVersion('maven', …) instead of node-semver.
export interface MavenRange {
  introduced: string | null
  fixed: string | null
  lastAffected: string | null
}

export function mavenRangeEvents(entry: OsvAffectedPackage): MavenRange[] {
  const events: MavenRange[] = []

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

function compareOrNull(a: string, b: string): number | null {
  return compareVersion('maven', a, b)
}

// Unparseable bound → treated as NOT in range (unlike mavenConstraint.ts's over-inclusive
// stance — this only decides the vulnerable-version set, not dependent reachability).
function isInRange(version: string, range: MavenRange): boolean {
  if (range.introduced) {
    const c = compareOrNull(version, range.introduced)
    if (c === null || c < 0) return false
  }
  if (range.fixed) {
    const c = compareOrNull(version, range.fixed)
    if (c === null || c >= 0) return false
  }
  if (range.lastAffected) {
    const c = compareOrNull(version, range.lastAffected)
    if (c === null || c > 0) return false
  }
  return true
}

export function versionsInRanges(versions: string[], ranges: MavenRange[]): string[] {
  const result: string[] = []
  for (const v of versions) {
    for (const range of ranges) {
      if (isInRange(v, range)) {
        result.push(v)
        break
      }
    }
  }
  return result
}

export function highestVersion(versions: string[]): string | null {
  let best: string | null = null
  for (const v of versions) {
    if (best === null) {
      best = v
      continue
    }
    const c = compareOrNull(v, best)
    if (c !== null && c > 0) best = v
  }
  return best
}
