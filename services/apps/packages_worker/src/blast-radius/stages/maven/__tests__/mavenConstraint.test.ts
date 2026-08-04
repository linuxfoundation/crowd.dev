import { describe, expect, it } from 'vitest'

import { mavenConstraintMayInclude, mavenDependencyMayIncludeVuln } from '../mavenConstraint'

describe('mavenConstraintMayInclude', () => {
  it('conservatively includes a bare soft-requirement version instead of treating it as a floor', () => {
    // Maven bare versions are recommendations, not enforced ranges — mediation elsewhere in
    // the tree can resolve to any other version, so ">= 1.0" can't be assumed.
    expect(mavenConstraintMayInclude('1.0', ['1.5'])).toBe('unparseable-included')
    expect(mavenConstraintMayInclude('1.5', ['1.5'])).toBe('unparseable-included')
    expect(mavenConstraintMayInclude('2.0', ['1.5'])).toBe('unparseable-included')
  })

  it('matches a half-open hard range containing the max vulnerable version', () => {
    expect(mavenConstraintMayInclude('[1.0,2.0)', ['1.5'])).toBe('matched')
  })

  it('excludes a half-open hard range that excludes the upper bound', () => {
    expect(mavenConstraintMayInclude('[1.0,2.0)', ['2.0'])).toBe('excluded')
  })

  it('matches a closed hard range including its upper bound', () => {
    expect(mavenConstraintMayInclude('[1.0,2.0]', ['2.0'])).toBe('matched')
  })

  it('matches an open-ended lower-bound range', () => {
    expect(mavenConstraintMayInclude('(,1.0]', ['1.0'])).toBe('matched')
  })

  it('excludes an open-ended lower-bound range above the max vulnerable version', () => {
    expect(mavenConstraintMayInclude('(,1.0]', ['1.5'])).toBe('excluded')
  })

  it('matches an open-ended upper-bound range', () => {
    expect(mavenConstraintMayInclude('[1.5,)', ['1.5'])).toBe('matched')
  })

  it('matches an exact-version range', () => {
    expect(mavenConstraintMayInclude('[1.0]', ['1.0'])).toBe('matched')
  })

  it('excludes an exact-version range that does not equal the max vulnerable version', () => {
    expect(mavenConstraintMayInclude('[1.0]', ['1.5'])).toBe('excluded')
  })

  it('matches when any interval in a comma-separated union matches', () => {
    expect(mavenConstraintMayInclude('[1.0,2.0),[3.0,4.0)', ['3.5'])).toBe('matched')
  })

  it('excludes when no interval in a comma-separated union matches', () => {
    expect(mavenConstraintMayInclude('[1.0,2.0),[3.0,4.0)', ['2.5'])).toBe('excluded')
  })

  it('conservatively includes an empty constraint', () => {
    expect(mavenConstraintMayInclude('', ['1.5'])).toBe('unparseable-included')
  })

  it('conservatively includes a malformed bracket expression', () => {
    expect(mavenConstraintMayInclude('[1.0,', ['1.5'])).toBe('unparseable-included')
  })

  it('matches a bounded range containing an older vulnerable version but not the max', () => {
    expect(mavenConstraintMayInclude('[1.0,1.2]', ['1.1', '2.9'])).toBe('matched')
  })

  it('matches an exact-version range equal to an older vulnerable version, not the max', () => {
    expect(mavenConstraintMayInclude('[1.0]', ['1.0', '2.9'])).toBe('matched')
  })

  it('conservatively includes a mismatched-bracket exact range instead of treating it as exact', () => {
    expect(mavenConstraintMayInclude('(1.0)', ['1.5'])).toBe('unparseable-included')
    expect(mavenConstraintMayInclude('[1.0)', ['1.5'])).toBe('unparseable-included')
  })

  it('conservatively includes a null constraint instead of throwing', () => {
    expect(mavenConstraintMayInclude(null, ['1.5'])).toBe('unparseable-included')
  })

  it('conservatively includes a range with trailing garbage after the closed interval', () => {
    // Must not silently accept the "[1.0,2.0)" prefix and exclude 3.0 — the malformed
    // trailing text makes the whole constraint unparseable.
    expect(mavenConstraintMayInclude('[1.0,2.0)garbage', ['3.0'])).toBe('unparseable-included')
  })

  it('conservatively includes a range with a dangling unmatched opening bracket', () => {
    expect(mavenConstraintMayInclude('[1.0,2.0', ['3.0'])).toBe('unparseable-included')
  })
})

describe('mavenDependencyMayIncludeVuln', () => {
  it('matches when the resolved version equals a vulnerable version', () => {
    expect(mavenDependencyMayIncludeVuln('1.5', '[9.0,)', ['1.5'])).toBe('matched')
  })

  it('excludes a resolved version outside the vulnerable set even if the declared range would match', () => {
    // Mediation elsewhere in the tree can override even a hard declared range — the
    // concrete resolved version wins over the constraint.
    expect(mavenDependencyMayIncludeVuln('5.0', '[1.0,10.0)', ['1.5'])).toBe('excluded')
  })

  it('conservatively includes an unparseable resolved version', () => {
    // compareMaven only returns null for punctuation-only/empty input (see versionCompare.ts) —
    // a plain string like "not-a-version" still tokenizes and compares fine.
    expect(mavenDependencyMayIncludeVuln('---', '[9.0,)', ['1.5'])).toBe('unparseable-included')
  })

  it('falls back to the declared constraint when resolution is unavailable', () => {
    expect(mavenDependencyMayIncludeVuln(null, '[1.0,2.0)', ['1.5'])).toBe('matched')
  })

  it('conservatively includes a bare soft-requirement constraint when resolution is unavailable', () => {
    expect(mavenDependencyMayIncludeVuln(null, '1.0', ['1.5'])).toBe('unparseable-included')
  })
})
