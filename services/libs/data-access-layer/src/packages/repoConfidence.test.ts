import { describe, expect, it } from 'vitest'

import {
  CONFIDENCE_HIGH_THRESHOLD,
  CONFIDENCE_MEDIUM_THRESHOLD,
  packageRepoConfidenceCall,
  packageRepoConfidenceLabel,
  packageRepoLinkClaimParams,
} from './repoConfidence'

describe('packageRepoConfidenceLabel', () => {
  it('labels on the exported thresholds', () => {
    expect(packageRepoConfidenceLabel(CONFIDENCE_HIGH_THRESHOLD)).toBe('high')
    expect(packageRepoConfidenceLabel(0.99)).toBe('high')
    expect(packageRepoConfidenceLabel(CONFIDENCE_MEDIUM_THRESHOLD)).toBe('medium')
    expect(packageRepoConfidenceLabel(0.79)).toBe('medium')
    expect(packageRepoConfidenceLabel(0.49)).toBe('low')
  })

  it('keeps the uniqueness offset inside the label band', () => {
    expect(packageRepoConfidenceLabel(0.803999999)).toBe('high')
    expect(packageRepoConfidenceLabel(0.503999999)).toBe('medium')
  })
})

describe('packageRepoLinkClaimParams', () => {
  it('defaults provenance for sources that carry none', () => {
    expect(packageRepoLinkClaimParams({ source: 'declared' })).toEqual({
      source: 'declared',
      provenance: null,
    })
  })

  it('passes an explicit claim through untouched', () => {
    expect(
      packageRepoLinkClaimParams({
        source: 'deps_dev',
        provenance: 'SLSA_ATTESTATION',
      }),
    ).toEqual({
      source: 'deps_dev',
      provenance: 'SLSA_ATTESTATION',
    })
  })
})

describe('packageRepoConfidenceCall', () => {
  it('binds a new claim to parameters by default', () => {
    const sql = packageRepoConfidenceCall('p', 'r')
    expect(sql).toContain('$(source)')
    expect(sql).toContain('$(provenance)')
    expect(sql).toContain('p.ecosystem')
    expect(sql).toContain('r.archived')
  })

  it('reads a rescored claim off the stored row', () => {
    const sql = packageRepoConfidenceCall('p', 'r', {
      source: 'pr.source',
      provenance: 'pr.provenance',
    })
    expect(sql).not.toContain('$(')
    expect(sql).toContain('pr.provenance')
  })
})
