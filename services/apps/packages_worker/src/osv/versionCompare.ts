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

// Mirrors Gem::Version::VERSION_PATTERN / ANCHORED_VERSION_PATTERN — a hyphen
// only ever introduces a single trailing prerelease group.
const RUBYGEMS_VERSION_PATTERN =
  /^\s*[0-9]+(\.[0-9a-zA-Z]+)*(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?\s*$/

// Splits a version into alternating digit/letter runs, per Gem::Version#segments.
// A hyphen is a prerelease marker (Gem::Version#initialize rewrites "-" to
// ".pre." before tokenizing), not a plain separator. Numeric runs use bigint
// so segments beyond Number.MAX_SAFE_INTEGER still compare exactly.
function toRubyGemsSegments(version: string): (bigint | string)[] {
  if (!RUBYGEMS_VERSION_PATTERN.test(version)) return []
  const normalized = version.replace(/-/g, '.pre.')
  const runs = normalized.match(/[0-9]+|[a-zA-Z]+/g) ?? []
  return runs.map((run) => (/^[0-9]+$/.test(run) ? BigInt(run) : run))
}

// Mirrors Gem::Version#canonical_segments: drop trailing zero segments, then
// drop any zero segments that sit directly before the first letter segment.
function canonicalRubyGemsSegments(segments: (bigint | string)[]): (bigint | string)[] {
  const trimmed = [...segments]
  while (trimmed.length > 1 && trimmed[trimmed.length - 1] === BigInt(0)) trimmed.pop()

  const firstLetterIndex = trimmed.findIndex((segment) => typeof segment === 'string')
  if (firstLetterIndex === -1) return trimmed

  let zeroRunStart = firstLetterIndex
  while (zeroRunStart > 0 && trimmed[zeroRunStart - 1] === BigInt(0)) zeroRunStart--
  trimmed.splice(zeroRunStart, firstLetterIndex - zeroRunStart)

  return trimmed
}

// Compares one pair of segments the way Gem::Version#<=> does: a letter
// segment always sorts below a numeric segment, regardless of its value.
function compareRubyGemsSegment(lhs: bigint | string, rhs: bigint | string): number {
  if (typeof lhs === 'string' && typeof rhs !== 'string') return -1
  if (typeof lhs !== 'string' && typeof rhs === 'string') return 1
  return lhs < rhs ? -1 : lhs > rhs ? 1 : 0
}

// Once the shorter side is exhausted, Gem::Version#<=> walks the longer
// side's remaining segments: a letter segment means the longer side is a
// prerelease of the shorter one (sorts lower); a nonzero number means the
// longer side sorts higher; zeros (already mostly trimmed) are skipped.
function rubyGemsTailSign(segments: (bigint | string)[], startIndex: number): number {
  for (let i = startIndex; i < segments.length; i++) {
    const segment = segments[i]
    if (typeof segment === 'string') return -1
    if (segment !== BigInt(0)) return 1
  }
  return 0
}

function compareRubyGems(a: string, b: string): number | null {
  const aSegments = canonicalRubyGemsSegments(toRubyGemsSegments(a))
  const bSegments = canonicalRubyGemsSegments(toRubyGemsSegments(b))
  if (aSegments.length === 0 || bSegments.length === 0) return null

  const limit = Math.min(aSegments.length, bSegments.length)
  for (let i = 0; i < limit; i++) {
    if (aSegments[i] === bSegments[i]) continue
    return compareRubyGemsSegment(aSegments[i], bSegments[i])
  }

  if (aSegments.length > bSegments.length) return rubyGemsTailSign(aSegments, limit)
  if (bSegments.length > aSegments.length) return -rubyGemsTailSign(bSegments, limit)
  return 0
}

interface Pep440Pre {
  letter: 'a' | 'b' | 'rc'
  num: bigint
}

interface Pep440Version {
  epoch: bigint
  release: bigint[]
  pre: Pep440Pre | null
  post: bigint | null
  dev: bigint | null
  local: (bigint | string)[]
}

// Longest-alias-first: JS regex alternation picks the first matching alternative, not the
// longest, so "a" would swallow only the first letter of "alpha" if it came first.
const PEP440_PRE_ALIASES: Record<string, 'a' | 'b' | 'rc'> = {
  alpha: 'a',
  a: 'a',
  beta: 'b',
  b: 'b',
  preview: 'rc',
  pre: 'rc',
  rc: 'rc',
  c: 'rc',
}
const PEP440_PRE_RE = /^[-_.]?(alpha|beta|preview|pre|rc|a|b|c)[-_.]?([0-9]*)/
const PEP440_POST_RE = /^[-_.]?(?:post|rev|r)[-_.]?([0-9]*)/
const PEP440_DEV_RE = /^[-_.]?dev[-_.]?([0-9]*)/
const PEP440_LOCAL_RE = /^\+([a-z0-9]+(?:[-_.][a-z0-9]+)*)/

// https://peps.python.org/pep-0440/#version-scheme — consumes the grammar left to right;
// anything left unconsumed means the input isn't a valid PEP 440 version.
function parsePep440(raw: string): Pep440Version | null {
  let s = raw.trim().toLowerCase().replace(/^v/, '')

  let epoch = BigInt(0)
  const epochMatch = s.match(/^([0-9]+)!/)
  if (epochMatch) {
    epoch = BigInt(epochMatch[1])
    s = s.slice(epochMatch[0].length)
  }

  const releaseMatch = s.match(/^[0-9]+(?:\.[0-9]+)*/)
  if (!releaseMatch) return null
  const release = releaseMatch[0].split('.').map((n) => BigInt(n))
  s = s.slice(releaseMatch[0].length)

  let pre: Pep440Pre | null = null
  const preMatch = s.match(PEP440_PRE_RE)
  if (preMatch) {
    pre = {
      letter: PEP440_PRE_ALIASES[preMatch[1]],
      num: preMatch[2] ? BigInt(preMatch[2]) : BigInt(0),
    }
    s = s.slice(preMatch[0].length)
  }

  let post: bigint | null = null
  const implicitPostMatch = s.match(/^-([0-9]+)/)
  if (implicitPostMatch) {
    post = BigInt(implicitPostMatch[1])
    s = s.slice(implicitPostMatch[0].length)
  } else {
    const postMatch = s.match(PEP440_POST_RE)
    if (postMatch) {
      post = postMatch[1] ? BigInt(postMatch[1]) : BigInt(0)
      s = s.slice(postMatch[0].length)
    }
  }

  let dev: bigint | null = null
  const devMatch = s.match(PEP440_DEV_RE)
  if (devMatch) {
    dev = devMatch[1] ? BigInt(devMatch[1]) : BigInt(0)
    s = s.slice(devMatch[0].length)
  }

  let local: (bigint | string)[] = []
  const localMatch = s.match(PEP440_LOCAL_RE)
  if (localMatch) {
    local = localMatch[1].split(/[-_.]/).map((seg) => (/^[0-9]+$/.test(seg) ? BigInt(seg) : seg))
    s = s.slice(localMatch[0].length)
  }

  if (s.length > 0) return null

  return { epoch, release, pre, post, dev, local }
}

const PEP440_PRE_RANK: Record<'a' | 'b' | 'rc', bigint> = {
  a: BigInt(0),
  b: BigInt(1),
  rc: BigInt(2),
}

// Bare dev release sorts below pre-releases; release with no pre-release sorts above them —
// see PEP 440's documented order .devN < aN < bN < rcN < final < .postN.
function pep440PreOrder(v: Pep440Version): [bigint, bigint, bigint] {
  if (v.pre) return [BigInt(0), PEP440_PRE_RANK[v.pre.letter], v.pre.num]
  if (v.post === null && v.dev !== null) return [BigInt(-1), BigInt(0), BigInt(0)]
  return [BigInt(1), BigInt(0), BigInt(0)]
}

function comparePep440Local(a: (bigint | string)[], b: (bigint | string)[]): number {
  const len = Math.max(a.length, b.length)
  for (let i = 0; i < len; i++) {
    if (i >= a.length) return -1
    if (i >= b.length) return 1
    const [x, y] = [a[i], b[i]]
    if (typeof x === 'bigint' && typeof y === 'bigint') {
      if (x !== y) return x < y ? -1 : 1
      continue
    }
    if (typeof x === 'bigint') return 1
    if (typeof y === 'bigint') return -1
    if (x !== y) return x < y ? -1 : 1
  }
  return 0
}

function comparePep440(a: string, b: string): number | null {
  const pa = parsePep440(a)
  const pb = parsePep440(b)
  if (!pa || !pb) return null

  if (pa.epoch !== pb.epoch) return pa.epoch < pb.epoch ? -1 : 1

  const relLen = Math.max(pa.release.length, pb.release.length)
  for (let i = 0; i < relLen; i++) {
    const ra = pa.release[i] ?? BigInt(0)
    const rb = pb.release[i] ?? BigInt(0)
    if (ra !== rb) return ra < rb ? -1 : 1
  }

  const preA = pep440PreOrder(pa)
  const preB = pep440PreOrder(pb)
  for (let i = 0; i < 3; i++) {
    if (preA[i] !== preB[i]) return preA[i] < preB[i] ? -1 : 1
  }

  const postA = pa.post ?? BigInt(-1)
  const postB = pb.post ?? BigInt(-1)
  if (postA !== postB) return postA < postB ? -1 : 1

  const devA = pa.dev
  const devB = pb.dev
  if (devA !== devB) {
    if (devA === null) return 1
    if (devB === null) return -1
    return devA < devB ? -1 : 1
  }

  if (pa.local.length === 0 && pb.local.length === 0) return 0
  if (pa.local.length === 0) return -1
  if (pb.local.length === 0) return 1
  return comparePep440Local(pa.local, pb.local)
}

const SEMVER_ECOSYSTEMS = new Set(['npm', 'cargo', 'nuget', 'go'])

// Ecosystem names are stored lowercase in packages-db per ADR-0001 §OSV
// "Ecosystem normalization" — 'npm', 'maven', 'cargo'. Callers (deriveCriticalFlag)
// pull the value straight from the DB so the literals here must match.
export function compareVersion(ecosystem: string, a: string, b: string): number | null {
  if (SEMVER_ECOSYSTEMS.has(ecosystem)) return compareSemver(a, b)
  if (ecosystem === 'maven') return compareMaven(a, b)
  if (ecosystem === 'rubygems') return compareRubyGems(a, b)
  if (ecosystem === 'pypi') return comparePep440(a, b)
  return null
}
