import * as semver from 'semver'

// Ecosystem-specific version ordering. Returns -1, 0, 1 like Array#sort,
// or null when either operand cannot be parsed (we treat that as "do not match").

function coerceSemver(v: string): semver.SemVer | null {
  const parsed = semver.parse(v, { loose: true })
  if (parsed) return parsed
  return semver.coerce(v) ?? null
}

function compareNpm(a: string, b: string): number | null {
  const pa = coerceSemver(a)
  const pb = coerceSemver(b)
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

function compareMaven(a: string, b: string): number {
  const ta = tokenizeMaven(a)
  const tb = tokenizeMaven(b)
  const zero: Token = { kind: 'num', value: 0 }
  const max = Math.max(ta.length, tb.length)
  for (let i = 0; i < max; i++) {
    const aTok = i < ta.length ? ta[i] : zero
    const bTok = i < tb.length ? tb[i] : zero
    const c = cmpToken(aTok, bTok)
    if (c !== 0) return c
  }
  return 0
}

export function compareVersion(ecosystem: string, a: string, b: string): number | null {
  if (ecosystem === 'npm') return compareNpm(a, b)
  if (ecosystem === 'Maven') return compareMaven(a, b)
  return null
}
