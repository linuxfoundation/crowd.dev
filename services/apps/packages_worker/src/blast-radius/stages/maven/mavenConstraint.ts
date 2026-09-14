import { compareVersion } from '../../../osv/versionCompare'

export type MavenConstraintMatch = 'matched' | 'excluded' | 'unparseable-included'

interface MavenInterval {
  lower: string | null
  lowerInclusive: boolean
  upper: string | null
  upperInclusive: boolean
}

// A top-level comma joins alternative intervals, e.g. "[1.0,2.0),[3.0,4.0)" — track
// bracket depth only to skip the comma inside an interval's own lower/upper bound.
// Returns null (not a partial result) on unmatched brackets or trailing text after
// the last closed interval, e.g. "[1.0,2.0)garbage" — accepting the valid-looking
// prefix there would silently drop the "garbage" tail and could wrongly exclude a
// vulnerable version instead of falling through to unparseable-included.
function splitTopLevelCommas(s: string): string[] | null {
  const parts: string[] = []
  let depth = 0
  let start = 0
  for (let i = 0; i < s.length; i++) {
    if (s[i] === '[' || s[i] === '(') depth++
    else if (s[i] === ']' || s[i] === ')') {
      depth--
      if (depth < 0) return null
      if (depth === 0) {
        parts.push(s.slice(start, i + 1))
        start = i + 1
        if (s[start] === ',') start++
      }
    }
  }
  if (depth !== 0 || start !== s.length) return null
  return parts.filter(Boolean)
}

function parseMavenInterval(seg: string): MavenInterval | null {
  const s = seg.trim()
  if (s.length < 2) return null
  if (s[0] !== '[' && s[0] !== '(') return null
  if (s[s.length - 1] !== ']' && s[s.length - 1] !== ')') return null

  const lowerInclusive = s[0] === '['
  const upperInclusive = s[s.length - 1] === ']'
  const body = s.slice(1, -1)
  const comma = body.indexOf(',')

  if (comma === -1) {
    // Exact version, e.g. "[1.0]" — requires both brackets closed, unlike a mismatched
    // "(1.0)" or "[1.0)", which is malformed and must fall through to unparseable-included.
    if (!lowerInclusive || !upperInclusive) return null
    const exact = body.trim()
    if (!exact) return null
    return { lower: exact, lowerInclusive: true, upper: exact, upperInclusive: true }
  }

  const lower = body.slice(0, comma).trim() || null
  const upper = body.slice(comma + 1).trim() || null
  return { lower, lowerInclusive, upper, upperInclusive }
}

// A bare version (e.g. "1.5") is Maven's soft requirement, not an enforced floor — mediation
// elsewhere in the tree can override it, so it's treated as unparseable rather than ">= 1.5".
function parseMavenRange(constraint: string | null): MavenInterval[] | null {
  // package_dependencies.version_constraint is nullable (deps.dev fill path) — treat a
  // missing constraint the same as an unparseable one, not a crash on .trim().
  if (constraint == null) return null
  const trimmed = constraint.trim()
  if (!trimmed) return null
  if (!trimmed.startsWith('[') && !trimmed.startsWith('(')) return null

  const segments = splitTopLevelCommas(trimmed)
  if (!segments || segments.length === 0) return null
  const intervals: MavenInterval[] = []
  for (const seg of segments) {
    const interval = parseMavenInterval(seg)
    if (!interval) return null
    intervals.push(interval)
  }
  return intervals
}

function intervalMayInclude(interval: MavenInterval, version: string): boolean {
  if (interval.lower) {
    const c = compareVersion('maven', version, interval.lower)
    if (c === null) return true // unparseable bound — over-inclusive
    if (interval.lowerInclusive ? c < 0 : c <= 0) return false
  }
  if (interval.upper) {
    const c = compareVersion('maven', version, interval.upper)
    if (c === null) return true
    if (interval.upperInclusive ? c > 0 : c >= 0) return false
  }
  return true
}

// Over-inclusive by design: the reachability stage (real source analysis) is the
// actual precision filter, so an unparseable constraint is always surfaced, never dropped.
//
// Checks against every vulnerable version, not just the highest one: unlike Go's
// unbounded-above floors (where the max alone determines the match), Maven hard ranges
// like "[1.0,1.2]" or "[1.0]" are bounded and can include an older vulnerable version
// without including the max.
export function mavenConstraintMayInclude(
  constraint: string | null,
  vulnerableVersions: string[],
): MavenConstraintMatch {
  const intervals = parseMavenRange(constraint)
  if (!intervals) return 'unparseable-included'
  const matched = intervals.some((interval) =>
    vulnerableVersions.some((version) => intervalMayInclude(interval, version)),
  )
  return matched ? 'matched' : 'excluded'
}

// Prefers the edge's concrete resolved version over the declared constraint when known — mediation
// can override even an explicit range, and it's the only sound way to evaluate a bare requirement.
export function mavenDependencyMayIncludeVuln(
  resolvedVersion: string | null,
  constraint: string | null,
  vulnerableVersions: string[],
): MavenConstraintMatch {
  if (!resolvedVersion) return mavenConstraintMayInclude(constraint, vulnerableVersions)

  const comparisons = vulnerableVersions.map((version) =>
    compareVersion('maven', resolvedVersion, version),
  )
  if (comparisons.includes(0)) return 'matched'
  if (comparisons.includes(null)) return 'unparseable-included'
  return 'excluded'
}
