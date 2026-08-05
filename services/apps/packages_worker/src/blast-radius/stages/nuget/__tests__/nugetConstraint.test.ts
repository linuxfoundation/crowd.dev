import { describe, expect, it } from 'vitest'

import { nugetConstraintMayInclude } from '../nugetConstraint'

describe('nugetConstraintMayInclude', () => {
  it('treats a bare version as an inclusive floor, unlike Maven soft requirements', () => {
    // NuGet's resolver documents "1.0" as equivalent to "[1.0,)" — a real minimum,
    // not Maven's overridable soft hint.
    expect(nugetConstraintMayInclude('1.0.0', ['1.5.0'])).toBe('matched')
    expect(nugetConstraintMayInclude('1.5.0', ['1.5.0'])).toBe('matched')
    expect(nugetConstraintMayInclude('2.0.0', ['1.5.0'])).toBe('excluded')
  })

  it('matches a half-open hard range containing the max vulnerable version', () => {
    expect(nugetConstraintMayInclude('[1.0.0,2.0.0)', ['1.5.0'])).toBe('matched')
  })

  it('excludes a half-open hard range that excludes the upper bound', () => {
    expect(nugetConstraintMayInclude('[1.0.0,2.0.0)', ['2.0.0'])).toBe('excluded')
  })

  it('matches a closed hard range including its upper bound', () => {
    expect(nugetConstraintMayInclude('[1.0.0,2.0.0]', ['2.0.0'])).toBe('matched')
  })

  it('matches an open-ended lower-bound range', () => {
    expect(nugetConstraintMayInclude('(,1.0.0]', ['1.0.0'])).toBe('matched')
  })

  it('excludes an open-ended lower-bound range above the max vulnerable version', () => {
    expect(nugetConstraintMayInclude('(,1.0.0]', ['1.5.0'])).toBe('excluded')
  })

  it('matches an open-ended upper-bound range', () => {
    expect(nugetConstraintMayInclude('[1.5.0,)', ['1.5.0'])).toBe('matched')
  })

  it('matches an exact-version range', () => {
    expect(nugetConstraintMayInclude('[1.0.0]', ['1.0.0'])).toBe('matched')
  })

  it('excludes an exact-version range that does not equal the max vulnerable version', () => {
    expect(nugetConstraintMayInclude('[1.0.0]', ['1.5.0'])).toBe('excluded')
  })

  it('matches when any interval in a comma-separated union matches', () => {
    expect(nugetConstraintMayInclude('[1.0.0,2.0.0),[3.0.0,4.0.0)', ['3.5.0'])).toBe('matched')
  })

  it('excludes when no interval in a comma-separated union matches', () => {
    expect(nugetConstraintMayInclude('[1.0.0,2.0.0),[3.0.0,4.0.0)', ['2.5.0'])).toBe('excluded')
  })

  it('matches a bounded range containing an older vulnerable version but not the max', () => {
    expect(nugetConstraintMayInclude('[1.0.0,1.2.0]', ['1.1.0', '2.9.0'])).toBe('matched')
  })

  it('conservatively includes an empty constraint', () => {
    expect(nugetConstraintMayInclude('', ['1.5.0'])).toBe('unparseable-included')
  })

  it('conservatively includes a malformed bracket expression', () => {
    expect(nugetConstraintMayInclude('[1.0.0,', ['1.5.0'])).toBe('unparseable-included')
  })

  it('conservatively includes a mismatched-bracket exact range instead of treating it as exact', () => {
    expect(nugetConstraintMayInclude('(1.0.0)', ['1.5.0'])).toBe('unparseable-included')
    expect(nugetConstraintMayInclude('[1.0.0)', ['1.5.0'])).toBe('unparseable-included')
  })

  it('conservatively includes a null constraint instead of throwing', () => {
    expect(nugetConstraintMayInclude(null, ['1.5.0'])).toBe('unparseable-included')
  })

  it('conservatively includes a bare version containing bracket-like characters', () => {
    // Not a valid interval and not a plain version — must not be silently treated as a floor.
    expect(nugetConstraintMayInclude('1.0.0,2.0.0', ['1.5.0'])).toBe('unparseable-included')
  })

  it('conservatively includes a range with trailing garbage after the closed interval', () => {
    expect(nugetConstraintMayInclude('[1.0.0,2.0.0)garbage', ['3.0.0'])).toBe(
      'unparseable-included',
    )
  })
})
