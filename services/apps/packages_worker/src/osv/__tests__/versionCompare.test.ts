import { describe, expect, it } from 'vitest'

import { compareVersion } from '../versionCompare'

// Use sign() so tests assert ordering without hard-coding the exact value;
// the contract is -1/0/1, but we only care about the sign.
const sign = (n: number | null) => (n === null ? null : Math.sign(n))

describe('compareVersion — npm (semver)', () => {
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

  it('returns null for unparseable npm versions', () => {
    expect(compareVersion('npm', 'not-a-version-at-all', '1.0.0')).toBeNull()
  })

  it('coerces loose npm versions when possible', () => {
    // semver.coerce handles common "loose" inputs like trailing junk or missing patch.
    expect(sign(compareVersion('npm', '1.2', '1.3'))).toBe(-1)
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

describe('compareVersion — unsupported ecosystems', () => {
  it('returns null for ecosystems we have no comparator for', () => {
    expect(compareVersion('PyPI', '1.0.0', '2.0.0')).toBeNull()
    expect(compareVersion('crates.io', '0.1', '0.2')).toBeNull()
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
