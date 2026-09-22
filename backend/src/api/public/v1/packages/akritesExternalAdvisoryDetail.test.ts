import { describe, expect, it } from 'vitest'

import type { AkritesExternalAdvisoryRow } from '@crowd/data-access-layer'

import { toAkritesExternalAdvisoryDetail } from './akritesExternalAdvisoryDetail'

const purl = 'pkg:npm/lodash'

function row(overrides: Partial<AkritesExternalAdvisoryRow> = {}): AkritesExternalAdvisoryRow {
  return {
    purl,
    osvId: 'GHSA-1111-1111-1111',
    severity: 'critical',
    resolution: 'open',
    isCritical: true,
    ...overrides,
  }
}

describe('toAkritesExternalAdvisoryDetail', () => {
  it('maps well-formed advisory rows to the akrites-external shape', () => {
    const result = toAkritesExternalAdvisoryDetail(purl, [
      row({ osvId: 'GHSA-aaaa', severity: 'critical', resolution: 'open', isCritical: true }),
      row({ osvId: 'GHSA-bbbb', severity: 'high', resolution: 'patched', isCritical: false }),
    ])
    expect(result.purl).toBe(purl)
    expect(result.advisories).toEqual([
      { osvId: 'GHSA-aaaa', severity: 'critical', resolution: 'open', isCritical: true },
      { osvId: 'GHSA-bbbb', severity: 'high', resolution: 'patched', isCritical: false },
    ])
  })

  it('returns an empty advisories array for the found-but-advisory-less sentinel row', () => {
    // A found package with no advisories comes back as a single null-osvId row.
    const result = toAkritesExternalAdvisoryDetail(purl, [
      row({ osvId: null, severity: null, resolution: null, isCritical: null }),
    ])
    expect(result.advisories).toEqual([])
  })

  it('crosswalks the DB medium severity to the contract moderate value', () => {
    // The DB normalizes the middle band to MEDIUM; the contract calls it moderate.
    const result = toAkritesExternalAdvisoryDetail(purl, [row({ severity: 'medium' })])
    expect(result.advisories[0].severity).toBe('moderate')
  })

  it('coerces a severity outside the known vocabulary to null', () => {
    const result = toAkritesExternalAdvisoryDetail(purl, [row({ severity: 'info' })])
    expect(result.advisories[0].severity).toBeNull()
  })

  it('passes through null severity and null isCritical (unscored advisory)', () => {
    const result = toAkritesExternalAdvisoryDetail(purl, [
      row({ osvId: 'GHSA-cccc', severity: null, resolution: 'open', isCritical: null }),
    ])
    expect(result.advisories[0]).toEqual({
      osvId: 'GHSA-cccc',
      severity: null,
      resolution: 'open',
      isCritical: null,
    })
  })

  it('keeps the accepted moderate severity value', () => {
    const result = toAkritesExternalAdvisoryDetail(purl, [row({ severity: 'moderate' })])
    expect(result.advisories[0].severity).toBe('moderate')
  })
})
