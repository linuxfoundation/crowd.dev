import { describe, expect, it } from 'vitest'

import { pypiConstraintMayInclude, pypiDependencyMayIncludeVuln } from '../pypiConstraint'

describe('pypiConstraintMayInclude', () => {
  it('expands "~=" into a bounded range', () => {
    expect(pypiConstraintMayInclude('~=2.2', ['2.2.0', '2.3.0'])).toBe('matched')
    expect(pypiConstraintMayInclude('~=2.2', ['2.1.0'])).toBe('excluded')
    expect(pypiConstraintMayInclude('~=2.2', ['3.0.0'])).toBe('excluded')
    expect(pypiConstraintMayInclude('~=1.4.5', ['1.4.5', '1.4.9'])).toBe('matched')
    expect(pypiConstraintMayInclude('~=1.4.5', ['1.4.4'])).toBe('excluded')
    expect(pypiConstraintMayInclude('~=1.4.5', ['1.5.0'])).toBe('excluded')
  })

  it('treats "~=" with a single release segment as unparseable', () => {
    expect(pypiConstraintMayInclude('~=2', ['2.0.0'])).toBe('unparseable-included')
  })

  it('matches "==" with a ".*" wildcard as a prefix', () => {
    expect(pypiConstraintMayInclude('==2.2.*', ['2.2.0', '2.2.9'])).toBe('matched')
    expect(pypiConstraintMayInclude('==2.2.*', ['2.3.0'])).toBe('excluded')
  })

  it('matches "!=" with a ".*" wildcard as exclusion of the prefix', () => {
    expect(pypiConstraintMayInclude('!=2.2.*', ['2.3.0'])).toBe('matched')
    expect(pypiConstraintMayInclude('!=2.2.*', ['2.2.0'])).toBe('excluded')
  })

  it('matches exact "==" and "!="', () => {
    expect(pypiConstraintMayInclude('==1.0.0', ['1.0.0'])).toBe('matched')
    expect(pypiConstraintMayInclude('==1.0.0', ['1.0.1'])).toBe('excluded')
    expect(pypiConstraintMayInclude('!=1.0.0', ['1.0.1'])).toBe('matched')
    expect(pypiConstraintMayInclude('!=1.0.0', ['1.0.0'])).toBe('excluded')
  })

  it('matches "===" via arbitrary case-insensitive string equality, unnormalized', () => {
    expect(pypiConstraintMayInclude('===1.0.0', ['1.0.0'])).toBe('matched')
    expect(pypiConstraintMayInclude('===1.0.0', ['1.0.0.0'])).toBe('excluded')
  })

  it('ANDs comma-separated clauses', () => {
    expect(pypiConstraintMayInclude('>=1.0,<2.0', ['1.5.0'])).toBe('matched')
    expect(pypiConstraintMayInclude('>=1.0,<2.0', ['2.0.0'])).toBe('excluded')
    expect(pypiConstraintMayInclude('>=1.0,<2.0', ['0.9.0'])).toBe('excluded')
  })

  it('handles prerelease bounds', () => {
    expect(pypiConstraintMayInclude('<1.0', ['1.0rc1'])).toBe('matched')
    expect(pypiConstraintMayInclude('>=1.0', ['1.0rc1'])).toBe('excluded')
  })

  it('is over-inclusive on a null or malformed constraint', () => {
    expect(pypiConstraintMayInclude(null, ['1.0.0'])).toBe('unparseable-included')
    expect(pypiConstraintMayInclude('', ['1.0.0'])).toBe('unparseable-included')
    expect(pypiConstraintMayInclude('not-a-specifier', ['1.0.0'])).toBe('unparseable-included')
    expect(pypiConstraintMayInclude('>=1.0,', ['1.0.0'])).toBe('unparseable-included')
  })
})

describe('pypiDependencyMayIncludeVuln', () => {
  it('prefers the resolved version as ground truth', () => {
    expect(pypiDependencyMayIncludeVuln('1.0.0', '<1.0.0', ['1.0.0'])).toBe('matched')
    expect(pypiDependencyMayIncludeVuln('2.0.0', '<1.0.0', ['1.0.0'])).toBe('excluded')
  })

  it('falls back to the constraint when no resolved version is present', () => {
    expect(pypiDependencyMayIncludeVuln(null, '==1.0.0', ['1.0.0'])).toBe('matched')
    expect(pypiDependencyMayIncludeVuln(null, null, ['1.0.0'])).toBe('unparseable-included')
  })
})
