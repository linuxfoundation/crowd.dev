import * as semver from 'semver'

// Ecosystem-specific version ordering. Returns -1, 0, 1 like Array#sort,
// or null when either operand cannot be parsed (we treat that as "do not match").

// We deliberately do NOT fall back to semver.coerce. Coerce is lossy:
// "1.2-junk-3" becomes "1.2.3" and "v1" becomes "1.0.0", which can mint a
// false-positive range match that flips has_critical_vulnerability without
// evidence. semver.parse with { loose: true } already accepts the common
// shapes OSV emits (leading "v", prerelease tags); anything it rejects we
// treat as unparseable and isInRange interprets as "no match". Under-flag
// over mis-flag is the trade-off we want here.
function parseLoose(v: string): semver.SemVer | null {
  return semver.parse(v, { loose: true })
}

function compareSemver(a: string, b: string): number | null {
  const pa = parseLoose(a)
  const pb = parseLoose(b)
  if (!pa || !pb) return null
  return semver.compare(pa, pb)
}

// Minimal Maven ComparableVersion. Tokenizes on '.', '-', and on the boundary
// between numeric and alpha runs, then compares token-by-token with the standard
// Maven qualifier ranks. Handles the bulk of real-world versions (numeric dotted
// + common qualifiers); intentionally not bug-for-bug compatible with Maven's
// org.apache.maven.artifact.versioning.ComparableVersion.
type Token = { kind: 'num'; value: number } | { kind: 'str'; value: string }

function tokenizeMaven(version: string): Token[] {
  const out: Token[] = []
  const s = version.toLowerCase()
  let i = 0
  while (i < s.length) {
    const c = s[i]
    if (c === '.' || c === '-') {
      i++
      continue
    }
    if (c >= '0' && c <= '9') {
      let j = i
      while (j < s.length && s[j] >= '0' && s[j] <= '9') j++
      out.push({ kind: 'num', value: parseInt(s.slice(i, j), 10) })
      i = j
    } else {
      let j = i
      while (j < s.length && s[j] !== '.' && s[j] !== '-' && !(s[j] >= '0' && s[j] <= '9')) {
        j++
      }
      out.push({ kind: 'str', value: s.slice(i, j) })
      i = j
    }
  }
  return out
}

const MAVEN_QUALIFIER_RANK: Record<string, number> = {
  alpha: 0,
  a: 0,
  beta: 1,
  b: 1,
  milestone: 2,
  m: 2,
  rc: 3,
  cr: 3,
  snapshot: 4,
  '': 5,
  ga: 5,
  final: 5,
  release: 5,
  sp: 6,
}

function cmpToken(a: Token, b: Token): number {
  if (a.kind === 'num' && b.kind === 'num') {
    return a.value < b.value ? -1 : a.value > b.value ? 1 : 0
  }
  // Maven: numeric tokens rank above qualifier tokens at the same depth.
  if (a.kind === 'num') return 1
  if (b.kind === 'num') return -1
  const ar = MAVEN_QUALIFIER_RANK[a.value]
  const br = MAVEN_QUALIFIER_RANK[b.value]
  if (ar !== undefined && br !== undefined) {
    return ar < br ? -1 : ar > br ? 1 : 0
  }
  // Known qualifier ranks below unknown alpha tokens.
  if (ar !== undefined) return -1
  if (br !== undefined) return 1
  return a.value < b.value ? -1 : a.value > b.value ? 1 : 0
}

// When one side runs out of tokens, Maven's ComparableVersion substitutes a
// "null" token whose kind matches the *other* side at that position: num→0,
// str→'' (empty qualifier, ranked equal to 'ga'/'final'/'release'). Picking the
// kind unconditionally (e.g. always num:0) breaks comparisons like
// `1.0-final == 1.0` and `1.0 < 1.0-sp1`.
function padFor(other: Token): Token {
  return other.kind === 'num' ? { kind: 'num', value: 0 } : { kind: 'str', value: '' }
}

function compareMaven(a: string, b: string): number | null {
  const ta = tokenizeMaven(a)
  const tb = tokenizeMaven(b)
  // Empty or punctuation-only inputs (e.g. '', '...', '---') tokenize to []
  // and would otherwise be treated as version 0. Per the compareVersion
  // contract ("returns null when either operand cannot be parsed"), reject
  // them here so isInRange treats them as "no match" — safer than silently
  // ordering garbage as 0.
  if (ta.length === 0 || tb.length === 0) return null
  const max = Math.max(ta.length, tb.length)
  for (let i = 0; i < max; i++) {
    if (i >= ta.length) {
      const c = cmpToken(padFor(tb[i]), tb[i])
      if (c !== 0) return c
      continue
    }
    if (i >= tb.length) {
      const c = cmpToken(ta[i], padFor(ta[i]))
      if (c !== 0) return c
      continue
    }
    const c = cmpToken(ta[i], tb[i])
    if (c !== 0) return c
  }
  return 0
}

// Splits a version into alternating digit/letter runs, per Gem::Version#segments.
// Separators (dots, dashes, anything else) are simply discarded. Digit runs
// become numbers, letter runs stay strings.
function toRubyGemsSegments(version: string): (number | string)[] {
  const runs = version.match(/[0-9]+|[a-zA-Z]+/g) ?? []
  return runs.map((run) => (/^\d+$/.test(run) ? parseInt(run, 10) : run))
}

// Compares one pair of segments the way Gem::Version#<=> does: if either
// side is a letter run, both sides compare as strings; otherwise as numbers.
function compareRubyGemsSegment(lhs: number | string, rhs: number | string): number {
  if (typeof lhs === 'number' && typeof rhs === 'number') {
    return lhs < rhs ? -1 : lhs > rhs ? 1 : 0
  }
  const lhsStr = String(lhs)
  const rhsStr = String(rhs)
  return lhsStr < rhsStr ? -1 : lhsStr > rhsStr ? 1 : 0
}

function compareRubyGems(a: string, b: string): number | null {
  const aSegments = toRubyGemsSegments(a)
  const bSegments = toRubyGemsSegments(b)
  if (aSegments.length === 0 || bSegments.length === 0) return null

  // Missing trailing segments pad as 0, per Gem::Version#<=>.
  const segmentCount = Math.max(aSegments.length, bSegments.length)
  for (let i = 0; i < segmentCount; i++) {
    const lhs = aSegments[i] ?? 0
    const rhs = bSegments[i] ?? 0
    if (lhs === rhs) continue
    const cmp = compareRubyGemsSegment(lhs, rhs)
    if (cmp !== 0) return cmp
  }
  return 0
}

const SEMVER_ECOSYSTEMS = new Set(['npm', 'cargo', 'nuget'])

// Ecosystem names are stored lowercase in packages-db per ADR-0001 §OSV
// "Ecosystem normalization" — 'npm', 'maven', 'cargo'. Callers (deriveCriticalFlag)
// pull the value straight from the DB so the literals here must match.
export function compareVersion(ecosystem: string, a: string, b: string): number | null {
  if (SEMVER_ECOSYSTEMS.has(ecosystem)) return compareSemver(a, b)
  if (ecosystem === 'maven') return compareMaven(a, b)
  if (ecosystem === 'rubygems') return compareRubyGems(a, b)
  return null
}
