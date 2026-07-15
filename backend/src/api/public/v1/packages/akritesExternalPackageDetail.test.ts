import { describe, expect, it } from 'vitest'

import type { AkritesExternalPackageDetailRow } from '@crowd/data-access-layer'

import { toAkritesExternalPackageDetail } from './akritesExternalPackageDetail'

function baseRow(
  overrides: Partial<AkritesExternalPackageDetailRow> = {},
): AkritesExternalPackageDetailRow {
  return {
    purl: 'pkg:npm/lodash',
    name: 'lodash',
    ecosystem: 'npm',
    latestVersion: '4.17.21',
    versionsCount: 120,
    criticalityScore: 0.5,
    dependentPackagesCount: 100,
    dependentReposCount: 50,
    healthScore: 80,
    healthLabel: 'healthy',
    maintainerHealthScore: 70,
    securitySupplyChainScore: 90,
    developmentActivityScore: 60,
    signalCoverageHealth: null,
    lifecycleLabel: 'active',
    latestReleaseAt: null,
    hasCriticalVulnerability: false,
    declaredRepositoryUrl: null,
    repositoryUrl: null,
    maintainerCount: 1,
    resolvedRepositoryUrl: null,
    repoMappingConfidence: null,
    repoLastCommitAt: null,
    scorecardScore: 7.5,
    hasSecurityFile: true,
    hasSecurityPolicy: true,
    branchProtectionEnabled: true,
    pvrEnabled: true,
    securityPolicyUrl: null,
    vulnerabilityReportingUrl: null,
    bugBountyUrl: null,
    downloadsLast30d: '1000',
    ...overrides,
  }
}

describe('toAkritesExternalPackageDetail', () => {
  it('maps a well-formed row to the akrites-external shape', () => {
    const result = toAkritesExternalPackageDetail(baseRow())
    expect(result.purl).toBe('pkg:npm/lodash')
    expect(result.health.band).toBe('high')
    expect(result.riskSignals.lifecycle).toBe('active')
    expect(result.impact.score).toBe(50)
  })

  it('falls back to computeHealthBand(scorecardScore) for an unrecognized (non-null) healthLabel', () => {
    // scorecardScore 7.5 -> computeHealthBand() returns 'healthy' -> crosswalk 'high'.
    // A naive `?? computeHealthBand(...)` (bug fixed in code review) would have
    // trusted 'not-a-real-label' as-is and returned 'critical' instead.
    const result = toAkritesExternalPackageDetail(
      baseRow({ healthLabel: 'not-a-real-label', scorecardScore: 7.5 }),
    )
    expect(result.health.band).toBe('high')
  })

  it('falls back to computeHealthBand(scorecardScore) when healthLabel is null', () => {
    const result = toAkritesExternalPackageDetail(baseRow({ healthLabel: null, scorecardScore: 1 }))
    expect(result.health.band).toBe('critical')
  })

  it('maps every known internal health label through the crosswalk', () => {
    expect(toAkritesExternalPackageDetail(baseRow({ healthLabel: 'excellent' })).health.band).toBe(
      'high',
    )
    expect(toAkritesExternalPackageDetail(baseRow({ healthLabel: 'concerning' })).health.band).toBe(
      'low',
    )
    expect(toAkritesExternalPackageDetail(baseRow({ healthLabel: 'critical' })).health.band).toBe(
      'critical',
    )
  })

  it('returns null lifecycle for an unrecognized (non-null) lifecycleLabel instead of throwing', () => {
    const result = toAkritesExternalPackageDetail(baseRow({ lifecycleLabel: 'not-a-real-stage' }))
    expect(result.riskSignals.lifecycle).toBeNull()
  })

  it('maps lifecycle labels not present verbatim in the external enum (stable, archived)', () => {
    expect(
      toAkritesExternalPackageDetail(baseRow({ lifecycleLabel: 'stable' })).riskSignals.lifecycle,
    ).toBe('active')
    expect(
      toAkritesExternalPackageDetail(baseRow({ lifecycleLabel: 'archived' })).riskSignals.lifecycle,
    ).toBe('deprecated')
  })

  it('normalizes raw timestamptz strings (not Date objects) to ISO 8601', () => {
    // Timestamptz comes back from pg as a raw string, so a naive `.toISOString()`
    // on the row value would throw and 500 the request (bug fixed in code review).
    const result = toAkritesExternalPackageDetail(
      baseRow({
        latestReleaseAt: '2024-01-15 12:30:00+00',
        repoLastCommitAt: '2024-02-20 08:00:00+00',
      }),
    )
    expect(result.riskSignals.lastReleaseAt).toBe('2024-01-15T12:30:00.000Z')
    expect(result.provenance.lastCommitAt).toBe('2024-02-20T08:00:00.000Z')
  })

  it('falls back to the raw repositoryUrl column when resolvedRepositoryUrl has no package_repos link', () => {
    const result = toAkritesExternalPackageDetail(
      baseRow({ resolvedRepositoryUrl: null, repositoryUrl: 'https://github.com/example/repo' }),
    )
    expect(result.provenance.resolvedRepositoryUrl).toBe('https://github.com/example/repo')
  })

  it('prefers the confidence-joined resolvedRepositoryUrl over the raw repositoryUrl when both are present', () => {
    const result = toAkritesExternalPackageDetail(
      baseRow({
        resolvedRepositoryUrl: 'https://github.com/example/resolved',
        repositoryUrl: 'https://github.com/example/raw',
      }),
    )
    expect(result.provenance.resolvedRepositoryUrl).toBe('https://github.com/example/resolved')
  })
})
