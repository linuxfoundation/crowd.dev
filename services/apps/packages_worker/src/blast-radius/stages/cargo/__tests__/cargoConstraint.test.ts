import { describe, expect, it } from 'vitest'

import { cargoConstraintMayInclude, cargoDependencyMayIncludeVuln } from '../cargoConstraint'

describe('cargoConstraintMayInclude', () => {
  const vulnerable = ['1.2.0', '1.2.1', '1.3.0']

  it('treats a bare version as caret (matches within the same major)', () => {
    expect(cargoConstraintMayInclude('1.2.0', vulnerable)).toBe('matched')
  })

  it('excludes a bare version outside the caret range', () => {
    expect(cargoConstraintMayInclude('2.0.0', vulnerable)).toBe('excluded')
  })

  it('passes through explicit caret/tilde/equals operators', () => {
    expect(cargoConstraintMayInclude('^1.2', vulnerable)).toBe('matched')
    expect(cargoConstraintMayInclude('~1.2.0', vulnerable)).toBe('matched')
    expect(cargoConstraintMayInclude('=1.3.0', vulnerable)).toBe('matched')
    expect(cargoConstraintMayInclude('=9.9.9', vulnerable)).toBe('excluded')
  })

  it('supports wildcard requirements', () => {
    expect(cargoConstraintMayInclude('1.2.*', vulnerable)).toBe('matched')
  })

  it('supports comma-separated AND comparator ranges', () => {
    expect(cargoConstraintMayInclude('>=1.2.1, <1.3.0', vulnerable)).toBe('matched')
    expect(cargoConstraintMayInclude('>=1.4.0, <2.0.0', vulnerable)).toBe('excluded')
  })

  it('treats a bare prerelease version as caret, not an exact pin', () => {
    expect(cargoConstraintMayInclude('1.2.0-alpha.1', ['1.2.0-alpha.1'])).toBe('matched')
    expect(cargoConstraintMayInclude('1.2.0-alpha.1', ['1.2.4-alpha.2'])).toBe('matched')
    expect(cargoConstraintMayInclude('1.2.0-alpha.1', ['2.0.0'])).toBe('excluded')
  })

  it('treats unparseable requirements as included rather than dropping them', () => {
    expect(cargoConstraintMayInclude('git+https://example.com/crate', vulnerable)).toBe(
      'unparseable-included',
    )
    expect(cargoConstraintMayInclude('', vulnerable)).toBe('unparseable-included')
  })
})

describe('cargoDependencyMayIncludeVuln', () => {
  const vulnerable = ['1.2.0', '1.2.1']

  it('prefers the resolved version over the declared requirement when present', () => {
    expect(cargoDependencyMayIncludeVuln('1.2.0', '^2.0.0', vulnerable)).toBe('matched')
    expect(cargoDependencyMayIncludeVuln('9.9.9', '^1.2.0', vulnerable)).toBe('excluded')
  })

  it('falls back to the requirement when no resolved version is available', () => {
    expect(cargoDependencyMayIncludeVuln(null, '^1.2.0', vulnerable)).toBe('matched')
    expect(cargoDependencyMayIncludeVuln(null, '^3.0.0', vulnerable)).toBe('excluded')
  })
})
