import { compareVersion } from '../../../osv/versionCompare'

export type NuGetConstraintMatch = 'matched' | 'excluded' | 'unparseable-included'

interface NuGetInterval {
  lower: string | null
  lowerInclusive: boolean
  upper: string | null
  upperInclusive: boolean
}

// A top-level comma joins alternative intervals, e.g. "[1.0,2.0),[3.0,4.0)" — track
// bracket depth only to skip the comma inside an interval's own lower/upper bound.
// Returns null (not a partial result) on unmatched brackets or trailing text after
// the last closed interval — accepting a valid-looking prefix there could silently
// drop the tail and wrongly exclude a vulnerable version instead of falling through
// to unparseable-included.
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
        if (start < s.length) {
          // Exactly one comma must separate top-level intervals — no separator
          // (e.g. "[1,2)[3,4)") and a trailing comma are both malformed.
          if (s[start] !== ',') return null
          start++
          if (start >= s.length) return null
        }
      }
    }
  }
  if (depth !== 0 || start !== s.length) return null
  return parts.filter(Boolean)
}

function parseNuGetInterval(seg: string): NuGetInterval | null {
  const s = seg.trim()
  if (s.length < 2) return null
  if (s[0] !== '[' && s[0] !== '(') return null
  if (s[s.length - 1] !== ']' && s[s.length - 1] !== ')') return null

  const lowerInclusive = s[0] === '['
  const upperInclusive = s[s.length - 1] === ']'
  const body = s.slice(1, -1)
  const comma = body.indexOf(',')

  if (comma === -1) {
    // Exact version, e.g. "[1.0.0]" — requires both brackets closed, unlike a mismatched
    // "(1.0.0)" or "[1.0.0)", which is malformed and must fall through to unparseable-included.
    if (!lowerInclusive || !upperInclusive) return null
    const exact = body.trim()
    if (!exact) return null
    return { lower: exact, lowerInclusive: true, upper: exact, upperInclusive: true }
  }

  const lower = body.slice(0, comma).trim() || null
  const upper = body.slice(comma + 1).trim() || null
  return { lower, lowerInclusive, upper, upperInclusive }
}

// Unlike Maven's bare requirement (a soft hint mediation can override, so it's treated
// as unparseable), NuGet's bare version IS the enforced minimum — NuGet's own resolver
// documents "1.0" as equivalent to "[1.0,)". Parse it as an open-ended lower-bound interval
// rather than falling through to unparseable-included.
function parseBareVersionAsFloor(trimmed: string): NuGetInterval | null {
  if (!trimmed || /[[(\]),]/.test(trimmed)) return null
  return { lower: trimmed, lowerInclusive: true, upper: null, upperInclusive: false }
}

function parseNuGetRange(constraint: string | null): NuGetInterval[] | null {
  // package_dependencies.version_constraint is nullable (deps.dev fill path) — treat a
  // missing constraint the same as an unparseable one, not a crash on .trim().
  if (constraint == null) return null
  const trimmed = constraint.trim()
  if (!trimmed) return null

  if (!trimmed.startsWith('[') && !trimmed.startsWith('(')) {
    const bare = parseBareVersionAsFloor(trimmed)
    return bare ? [bare] : null
  }

  const segments = splitTopLevelCommas(trimmed)
  if (!segments || segments.length === 0) return null
  const intervals: NuGetInterval[] = []
  for (const seg of segments) {
    const interval = parseNuGetInterval(seg)
    if (!interval) return null
    intervals.push(interval)
  }
  return intervals
}

function intervalMayInclude(interval: NuGetInterval, version: string): boolean {
  if (interval.lower) {
    const c = compareVersion('nuget', version, interval.lower)
    if (c === null) return true // unparseable bound — over-inclusive
    if (interval.lowerInclusive ? c < 0 : c <= 0) return false
  }
  if (interval.upper) {
    const c = compareVersion('nuget', version, interval.upper)
    if (c === null) return true
    if (interval.upperInclusive ? c > 0 : c >= 0) return false
  }
  return true
}

// Over-inclusive by design: the reachability stage (real source analysis) is the actual
// precision filter, so an unparseable constraint is always surfaced, never dropped.
//
// Checks against every vulnerable version, not just the highest one: a bounded interval
// like "[1.0.0,1.2.0]" can include an older vulnerable version without including the max.
export function nugetConstraintMayInclude(
  constraint: string | null,
  vulnerableVersions: string[],
): NuGetConstraintMatch {
  const intervals = parseNuGetRange(constraint)
  if (!intervals) return 'unparseable-included'
  const matched = intervals.some((interval) =>
    vulnerableVersions.some((version) => intervalMayInclude(interval, version)),
  )
  return matched ? 'matched' : 'excluded'
}
