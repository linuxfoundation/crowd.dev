import { describe, expect, it } from 'vitest'

import { rubygemsConstraintMayInclude } from '../rubygemsConstraint'

describe('rubygemsConstraintMayInclude', () => {
  it("treats a bare version as exact equality, unlike NuGet's inclusive floor", () => {
    expect(rubygemsConstraintMayInclude('1.0.0', ['1.0.0'])).toBe('matched')
    expect(rubygemsConstraintMayInclude('1.0.0', ['1.5.0'])).toBe('excluded')
  })

  it('matches ">=" and excludes below it', () => {
    expect(rubygemsConstraintMayInclude('>= 1.0.0', ['1.5.0'])).toBe('matched')
    expect(rubygemsConstraintMayInclude('>= 2.0.0', ['1.5.0'])).toBe('excluded')
  })

  it('matches "<" and excludes at/above it', () => {
    expect(rubygemsConstraintMayInclude('< 2.0.0', ['1.5.0'])).toBe('matched')
    expect(rubygemsConstraintMayInclude('< 1.0.0', ['1.5.0'])).toBe('excluded')
  })

  it('matches "!=" for anything but the excluded version', () => {
    expect(rubygemsConstraintMayInclude('!= 1.0.0', ['1.5.0'])).toBe('matched')
    expect(rubygemsConstraintMayInclude('!= 1.5.0', ['1.5.0'])).toBe('excluded')
  })

  it('ANDs comma-separated clauses', () => {
    expect(rubygemsConstraintMayInclude('>= 1.0.0, < 2.0.0', ['1.5.0'])).toBe('matched')
    expect(rubygemsConstraintMayInclude('>= 1.0.0, < 2.0.0', ['2.0.0'])).toBe('excluded')
  })

  it('expands "~>" with two segments to a floor/ceiling pair on the first segment', () => {
    // Per RubyGems' documented pessimistic-operator semantics, "~> 1.2" means
    // ">= 1.2, < 2.0" (bumps the major, not the minor) — only a 3-segment "~>" bumps
    // the minor. See https://guides.rubygems.org/patterns/#pessimistic-version-constraint.
    expect(rubygemsConstraintMayInclude('~> 1.2', ['1.9.5'])).toBe('matched')
    expect(rubygemsConstraintMayInclude('~> 1.2', ['2.0.0'])).toBe('excluded')
    expect(rubygemsConstraintMayInclude('~> 1.2', ['1.1.9'])).toBe('excluded')
  })

  it('expands "~>" with three segments to a floor/ceiling pair on the third segment', () => {
    expect(rubygemsConstraintMayInclude('~> 1.2.3', ['1.2.9'])).toBe('matched')
    expect(rubygemsConstraintMayInclude('~> 1.2.3', ['1.3.0'])).toBe('excluded')
    expect(rubygemsConstraintMayInclude('~> 1.2.3', ['1.2.2'])).toBe('excluded')
  })

  it('matches four-segment versions, which node-semver cannot parse', () => {
    expect(rubygemsConstraintMayInclude('< 3.0.9.1', ['3.0.9.0'])).toBe('matched')
    expect(rubygemsConstraintMayInclude('< 3.0.9.1', ['3.0.9.1'])).toBe('excluded')
  })

  it('matches a bounded range containing an older vulnerable version but not the max', () => {
    expect(rubygemsConstraintMayInclude('>= 1.0.0, < 1.2.0', ['1.1.0', '2.9.0'])).toBe('matched')
  })

  it('conservatively includes an empty constraint', () => {
    expect(rubygemsConstraintMayInclude('', ['1.5.0'])).toBe('unparseable-included')
  })

  it('conservatively includes a null constraint instead of throwing', () => {
    expect(rubygemsConstraintMayInclude(null, ['1.5.0'])).toBe('unparseable-included')
  })

  it('conservatively includes a malformed "~>" version', () => {
    expect(rubygemsConstraintMayInclude('~> abc', ['1.5.0'])).toBe('unparseable-included')
    expect(rubygemsConstraintMayInclude('~> 1', ['1.5.0'])).toBe('unparseable-included')
  })

  it('conservatively includes a clause with a missing version', () => {
    expect(rubygemsConstraintMayInclude('>=', ['1.5.0'])).toBe('unparseable-included')
    expect(rubygemsConstraintMayInclude('>', ['1.5.0'])).toBe('unparseable-included')
  })

  it('conservatively includes a trailing dangling comma', () => {
    expect(rubygemsConstraintMayInclude('>= 1.0.0,', ['1.5.0'])).toBe('unparseable-included')
  })
})
