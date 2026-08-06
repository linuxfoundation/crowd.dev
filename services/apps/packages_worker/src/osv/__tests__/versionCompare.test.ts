import { describe, expect, it } from 'vitest'

import { compareVersion } from '../versionCompare'

// Use sign() so tests assert ordering without hard-coding the exact value;
// the contract is -1/0/1, but we only care about the sign.
const sign = (n: number | null) => (n === null ? null : Math.sign(n))

describe('compareVersion — npm/cargo (semver)', () => {
  it.each([
    ['1.2.3', '1.2.4', -1],
    ['1.2.4', '1.2.3', 1],
    ['1.2.3', '1.2.3', 0],
    ['1.10.0', '1.9.0', 1], // numeric, not lex
    ['1.0.0-alpha', '1.0.0', -1], // prerelease < release
    ['1.0.0-alpha', '1.0.0-beta', -1],
    ['1.0.0+meta', '1.0.0', 0], // build metadata ignored
    ['4.17.20', '4.17.21', -1], // lodash CVE-2021-23337 boundary
  ])('compareVersion("npm", %s, %s) sign = %s', (a, b, expected) => {
    expect(sign(compareVersion('npm', a, b))).toBe(expected)
  })

  it.each([
    ['0.1.0', '0.2.0', -1],
    ['1.0.0', '1.0.0', 0],
    ['2.0.0', '1.9.9', 1],
    ['0.3.0-alpha.1', '0.3.0', -1],
  ])('compareVersion("cargo", %s, %s) sign = %s', (a, b, expected) => {
    expect(sign(compareVersion('cargo', a, b))).toBe(expected)
  })

  it('returns null for unparseable semver versions', () => {
    expect(compareVersion('npm', 'not-a-version-at-all', '1.0.0')).toBeNull()
    expect(compareVersion('cargo', 'not-a-version-at-all', '1.0.0')).toBeNull()
  })

  it('returns null for short / lossy versions instead of coercing', () => {
    // Under-flag over mis-flag: semver.coerce would map "1.2" → "1.2.0" and
    // "1.2-junk-3" → "1.2.3", which can flip has_critical_vulnerability on
    // a malformed introduced/fixed boundary. We prefer null (no match).
    expect(compareVersion('npm', '1.2', '1.3')).toBeNull()
    expect(compareVersion('npm', '1.2-junk-3', '1.2.4')).toBeNull()
    expect(compareVersion('npm', 'v1', '1.0.0')).toBeNull()
  })
})

describe('compareVersion — maven (ComparableVersion-style)', () => {
  it.each([
    // Basic numeric ordering
    ['1.0', '1.1', -1],
    ['1.1', '1.0', 1],
    ['1.0', '1.0', 0],
    ['1.0', '1.0.0', 0], // implicit zero padding
    ['1.0', '1.10', -1], // numeric, not lex
    // log4shell range boundaries
    ['2.14.1', '2.15.0', -1],
    ['2.15.0', '2.15.0', 0],
    ['2.16.0', '2.15.0', 1],
    // Qualifier ranks
    ['1.0-alpha', '1.0-beta', -1],
    ['1.0-beta', '1.0-rc', -1],
    ['1.0-rc', '1.0', -1], // rc < ga
    ['1.0-snapshot', '1.0', -1],
    ['1.0', '1.0-sp1', -1], // sp > ga
    ['1.0-final', '1.0', 0], // final == ga
    ['1.0-ga', '1.0', 0],
    // Qualifier aliases (single-letter forms)
    ['1.0-a', '1.0-alpha', 0],
    ['1.0-b', '1.0-beta', 0],
    ['1.0-m1', '1.0-milestone1', 0],
    // Numeric beats alpha at same depth
    ['1.0-1', '1.0-alpha', 1],
  ])('compareVersion("maven", %s, %s) sign = %s', (a, b, expected) => {
    expect(sign(compareVersion('maven', a, b))).toBe(expected)
  })

  it('returns null for unparseable maven versions', () => {
    // Empty / punctuation-only strings tokenize to [] and used to silently
    // compare as version 0. Per the comparator contract, return null instead.
    expect(compareVersion('maven', '', '1.0')).toBeNull()
    expect(compareVersion('maven', '1.0', '')).toBeNull()
    expect(compareVersion('maven', '...', '1.0')).toBeNull()
  })
})

describe('compareVersion — nuget (semver)', () => {
  it.each([
    ['1.0.0', '2.0.0', -1],
    ['2.0.0', '1.0.0', 1],
    ['1.0.0', '1.0.0', 0],
    ['1.10.0', '1.9.0', 1], // numeric, not lex
    ['1.0.0-alpha', '1.0.0', -1], // prerelease < release
    ['1.0.0-beta', '1.0.0-rc', -1],
    // Real-world CVE boundary: Newtonsoft.Json < 13.0.1 deserialization vuln
    ['13.0.0', '13.0.1', -1],
    ['13.0.1', '13.0.1', 0],
    ['13.0.2', '13.0.1', 1],
  ])('compareVersion("nuget", %s, %s) sign = %s', (a, b, expected) => {
    expect(sign(compareVersion('nuget', a, b))).toBe(expected)
  })

  it('returns null for unparseable nuget versions', () => {
    expect(compareVersion('nuget', 'not-a-version', '1.0.0')).toBeNull()
  })

  it('rejects titlecase "NuGet" — production storage is always lowercase', () => {
    // OSV records arrive with ecosystem="NuGet"; parseOsvRecord lowercases to
    // "nuget" before writing to packages-db. The comparator is keyed on the
    // same lowercase form. A titlecase call means the caller skipped normalization.
    expect(compareVersion('NuGet', '1.0.0', '2.0.0')).toBeNull()
  })
})

describe('compareVersion — go (semver)', () => {
  it.each([
    ['v1.2.3', 'v1.2.4', -1],
    ['v1.2.4', 'v1.2.3', 1],
    ['v1.2.3', 'v1.2.3', 0],
    ['v1.10.0', 'v1.9.0', 1], // numeric, not lex
    ['v1.0.0-alpha', 'v1.0.0', -1], // prerelease < release
    ['v0.0.0-20220314234659-1baeb1ce4c0b', 'v0.0.0-20220315000000-2caec2d5d1c1', -1], // pseudo-versions
  ])('compareVersion("go", %s, %s) sign = %s', (a, b, expected) => {
    expect(sign(compareVersion('go', a, b))).toBe(expected)
  })

  it('returns null for unparseable go versions', () => {
    expect(compareVersion('go', 'not-a-version', 'v1.0.0')).toBeNull()
  })

  it('rejects titlecase "Go" — production storage is always lowercase', () => {
    expect(compareVersion('Go', 'v1.0.0', 'v2.0.0')).toBeNull()
  })
})

describe('compareVersion — rubygems (Gem::Version-style)', () => {
  it.each([
    ['1.0.0', '1.0.1', -1],
    ['1.0.1', '1.0.0', 1],
    ['1.0.0', '1.0.0', 0],
    ['1.10.0', '1.9.0', 1], // numeric, not lex
    ['1.0', '1.0.0', 0], // missing trailing segment pads as 0
    ['1.0.pre', '1.0', -1], // prerelease sorts below the corresponding release
    ['1.0.0.rc1', '1.0.0.rc2', -1],
    // rack CVE-2022-30123 boundary
    ['2.2.3', '2.2.3.1', -1],
    ['1.0-1', '1.0.pre.1', 0], // hyphen is a prerelease marker, same as ".pre."
    ['1.0.a10', '1.0.a9', 1], // letter+digit runs split: a10 -> a.10, a9 -> a.9
    ['1.99999999999999999999999999999999', '1.99999999999999999999999999999998', 1], // beyond Number.MAX_SAFE_INTEGER — must not collapse to equal
  ])('compareVersion("rubygems", %s, %s) sign = %s', (a, b, expected) => {
    expect(sign(compareVersion('rubygems', a, b))).toBe(expected)
  })

  it('returns null for unparseable rubygems versions', () => {
    expect(compareVersion('rubygems', '', '1.0.0')).toBeNull()
    expect(compareVersion('rubygems', '---', '1.0.0')).toBeNull()
    expect(compareVersion('rubygems', '1..2', '1.0.0')).toBeNull()
  })

  it('rejects titlecase "RubyGems" — production storage is always lowercase', () => {
    expect(compareVersion('RubyGems', '1.0.0', '2.0.0')).toBeNull()
  })
})

describe('compareVersion — unsupported ecosystems', () => {
  it('returns null for ecosystems we have no comparator for', () => {
    expect(compareVersion('PyPI', '1.0.0', '2.0.0')).toBeNull()
  })

  it('rejects titlecase "Maven" — production storage is always lowercase', () => {
    // Regression guard for the casing bug Fix 1 missed: deriveCriticalFlag
    // reads `ecosystem` from packages-db where it's lowercase. The comparator
    // is keyed on the same lowercase form per ADR-0001 §OSV "Ecosystem
    // normalization". A titlecase 'Maven' call indicates the caller forgot
    // to normalize.
    expect(compareVersion('Maven', '1.0', '2.0')).toBeNull()
  })
})
