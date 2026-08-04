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
function splitTopLevelCommas(s: string): string[] {
  const parts: string[] = []
  let depth = 0
  let start = 0
  for (let i = 0; i < s.length; i++) {
    if (s[i] === '[' || s[i] === '(') depth++
    else if (s[i] === ']' || s[i] === ')') {
      depth--
      if (depth === 0) {
        parts.push(s.slice(start, i + 1))
        start = i + 1
        if (s[start] === ',') start++
      }
    }
  }
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
    // Exact version, e.g. "[1.0]"
    const exact = body.trim()
    if (!exact) return null
    return { lower: exact, lowerInclusive: true, upper: exact, upperInclusive: true }
  }

  const lower = body.slice(0, comma).trim() || null
  const upper = body.slice(comma + 1).trim() || null
  return { lower, lowerInclusive, upper, upperInclusive }
}

// A soft requirement (bare version, e.g. "1.5") is Maven's recommended version, not an
// enforced range — modeled as a floor: an unbounded-above interval starting there.
function parseMavenRange(constraint: string): MavenInterval[] | null {
  const trimmed = constraint.trim()
  if (!trimmed) return null

  if (trimmed.startsWith('[') || trimmed.startsWith('(')) {
    const segments = splitTopLevelCommas(trimmed)
    if (segments.length === 0) return null
    const intervals: MavenInterval[] = []
    for (const seg of segments) {
      const interval = parseMavenInterval(seg)
      if (!interval) return null
      intervals.push(interval)
    }
    return intervals
  }

  return [{ lower: trimmed, lowerInclusive: true, upper: null, upperInclusive: false }]
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
export function mavenConstraintMayInclude(
  constraint: string,
  maxVulnerableVersion: string,
): MavenConstraintMatch {
  const intervals = parseMavenRange(constraint)
  if (!intervals) return 'unparseable-included'
  const matched = intervals.some((interval) => intervalMayInclude(interval, maxVulnerableVersion))
  return matched ? 'matched' : 'excluded'
}
