import { describe, expect, it } from 'vitest'

import { mavenConstraintMayInclude } from '../mavenConstraint'

describe('mavenConstraintMayInclude', () => {
  it('matches a soft version floor below the max vulnerable version', () => {
    expect(mavenConstraintMayInclude('1.0', ['1.5'])).toBe('matched')
  })

  it('matches a soft version floor equal to the max vulnerable version', () => {
    expect(mavenConstraintMayInclude('1.5', ['1.5'])).toBe('matched')
  })

  it('excludes a soft version floor above the max vulnerable version', () => {
    expect(mavenConstraintMayInclude('2.0', ['1.5'])).toBe('excluded')
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
    // parseMavenRange itself is the only source of 'unparseable-included' — a soft floor
    // whose bound compareVersion can't tokenize (e.g. "---") is still over-inclusive
    // via intervalMayInclude's null handling, which reports as 'matched', not this branch.
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
