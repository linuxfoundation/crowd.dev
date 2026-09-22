import { describe, expect, it } from 'vitest'

import { compareNuGetVersion } from '../nugetVersionCompare'

describe('compareNuGetVersion', () => {
  it('compares three-component versions like semver', () => {
    expect(compareNuGetVersion('1.2.3', '1.2.4')).toBe(-1)
    expect(compareNuGetVersion('1.2.4', '1.2.3')).toBe(1)
    expect(compareNuGetVersion('1.2.3', '1.2.3')).toBe(0)
  })

  it('compares four-component versions, which node-semver cannot parse at all', () => {
    expect(compareNuGetVersion('1.2.3.4', '1.2.3.5')).toBe(-1)
    expect(compareNuGetVersion('1.2.3.10', '1.2.3.9')).toBe(1)
    expect(compareNuGetVersion('4.5.0.0', '4.5.0.0')).toBe(0)
  })

  it('treats a missing revision component as 0', () => {
    expect(compareNuGetVersion('1.2.3', '1.2.3.0')).toBe(0)
    expect(compareNuGetVersion('1.2.3.1', '1.2.3')).toBe(1)
  })

  it('ranks a release above any prerelease of the same numeric version', () => {
    expect(compareNuGetVersion('1.0.0', '1.0.0-beta')).toBe(1)
    expect(compareNuGetVersion('1.0.0-beta', '1.0.0')).toBe(-1)
  })

  it('orders prerelease identifiers numerically, not lexically', () => {
    expect(compareNuGetVersion('1.0.0-beta.2', '1.0.0-beta.10')).toBe(-1)
  })

  it('ranks a shorter prerelease identifier list below a longer one sharing its prefix', () => {
    expect(compareNuGetVersion('1.0.0-beta', '1.0.0-beta.1')).toBe(-1)
  })

  it('ignores build metadata for comparison', () => {
    expect(compareNuGetVersion('1.0.0+build1', '1.0.0+build2')).toBe(0)
  })

  it('returns null for an unparseable version', () => {
    expect(compareNuGetVersion('not-a-version', '1.0.0')).toBeNull()
    expect(compareNuGetVersion('1.0.0', 'not-a-version')).toBeNull()
    expect(compareNuGetVersion('1.2.3.4.5', '1.0.0')).toBeNull()
  })
})
