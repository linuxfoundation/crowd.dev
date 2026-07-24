import {
  type AkritesExternalPackageDetailRow,
  type HealthBand,
  computeHealthBand,
} from '@crowd/data-access-layer'

import { repoMappingLabel, snakeToCamelKeys, toNullableNumber } from './mappers'
import { HEALTH_BAND_SET, LIFECYCLE_VALUES, type Lifecycle } from './types'

const LIFECYCLE_SET = new Set<string>(LIFECYCLE_VALUES)

// The API returns the internal bands verbatim — no external crosswalk (confirmed
// with product): health excellent/healthy/fair/concerning/critical, lifecycle
// active/stable/declining/abandoned/archived.
export type AkritesExternalHealthBand = HealthBand
export type AkritesExternalLifecycle = Lifecycle | null

export interface AkritesExternalPackageDetail {
  purl: string
  name: string
  ecosystem: string
  latestVersion: string | null
  versionCount: number | null
  health: {
    score: number | null
    band: AkritesExternalHealthBand
    subScores: {
      maintainerHealth: number | null
      securitySupplyChain: number | null
      developmentActivity: number | null
    }
    signalCoverageHealth: Record<string, unknown> | null
  }
  impact: {
    score: number | null
    downloadsLast30Days: string | null
    dependentPackagesCount: number | null
    dependentReposCount: number | null
    transitiveReach: null
  }
  riskSignals: {
    lifecycle: AkritesExternalLifecycle
    maintainerBusFactor: number | null
    lastReleaseAt: string | null
    hasSecurityFile: boolean | null
    hasSecurityPolicy: boolean | null
    branchProtectionEnabled: boolean | null
    openssfScorecardScore: number | null
  }
  security: {
    securityPolicyUrl: string | null
    vulnerabilityReportingUrl: string | null
    bugBountyUrl: string | null
    pvrEnabled: boolean | null
    criticalVulnerabilityFlag: boolean
  }
  provenance: {
    resolvedRepositoryUrl: string | null
    declaredRepositoryUrl: string | null
    mappingConfidenceScore: number | null
    mappingConfidenceLabel: 'High' | 'Medium' | 'Low' | null
    lastCommitAt: string | null
  }
  supplyChainIntegrity: {
    buildProvenance: null
    signedReleases: null
  }
}

export interface PackageDetailBulkEntry {
  requestedPurl: string
  found: boolean
  package: AkritesExternalPackageDetail | null
}

function toAkritesHealthBand(
  healthLabel: string | null,
  scorecardScore: number | null,
): AkritesExternalHealthBand {
  // Same validity guard as getPackage.ts: an unrecognized (not just null) stored
  // label falls back to computeHealthBand() instead of silently miscategorizing.
  return healthLabel != null && HEALTH_BAND_SET.has(healthLabel)
    ? (healthLabel as HealthBand)
    : computeHealthBand(scorecardScore)
}

function toAkritesLifecycle(lifecycleLabel: string | null): AkritesExternalLifecycle {
  if (lifecycleLabel === null || !LIFECYCLE_SET.has(lifecycleLabel)) return null
  return lifecycleLabel as Lifecycle
}

// Timestamptz columns arrive as the raw Postgres string (OID 1184 parser returns
// it verbatim — see AkritesExternalPackageDetailRow), so normalize to canonical
// ISO 8601 for the contract's date-time fields. Returns null for null/unparseable.
function toIsoOrNull(value: string | null): string | null {
  if (value == null) return null
  const ms = Date.parse(value)
  return Number.isNaN(ms) ? null : new Date(ms).toISOString()
}

export function toAkritesExternalPackageDetail(
  row: AkritesExternalPackageDetailRow,
): AkritesExternalPackageDetail {
  const scorecardScore = row.scorecardScore != null ? Number(row.scorecardScore) : null
  const mappingConfidence = toNullableNumber(row.repoMappingConfidence)

  return {
    purl: row.purl,
    name: row.name,
    ecosystem: row.ecosystem,
    latestVersion: row.latestVersion ?? null,
    versionCount: row.versionsCount ?? null,
    health: {
      score: row.healthScore,
      band: toAkritesHealthBand(row.healthLabel, scorecardScore),
      subScores: {
        maintainerHealth: row.maintainerHealthScore,
        securitySupplyChain: row.securitySupplyChainScore,
        developmentActivity: row.developmentActivityScore,
      },
      signalCoverageHealth: snakeToCamelKeys(row.signalCoverageHealth),
    },
    impact: {
      score: row.criticalityScore != null ? Math.round(row.criticalityScore * 100) : null,
      downloadsLast30Days: row.downloadsLast30d ?? null,
      dependentPackagesCount: row.dependentPackagesCount ?? null,
      dependentReposCount: row.dependentReposCount ?? null,
      transitiveReach: null,
    },
    riskSignals: {
      lifecycle: toAkritesLifecycle(row.lifecycleLabel),
      maintainerBusFactor: row.maintainerCount,
      lastReleaseAt: toIsoOrNull(row.latestReleaseAt),
      hasSecurityFile: row.hasSecurityFile ?? null,
      hasSecurityPolicy: row.hasSecurityPolicy ?? null,
      branchProtectionEnabled: row.branchProtectionEnabled ?? null,
      openssfScorecardScore: scorecardScore,
    },
    security: {
      securityPolicyUrl: row.securityPolicyUrl ?? null,
      vulnerabilityReportingUrl: row.vulnerabilityReportingUrl ?? null,
      bugBountyUrl: row.bugBountyUrl ?? null,
      pvrEnabled: row.pvrEnabled ?? null,
      criticalVulnerabilityFlag: row.hasCriticalVulnerability,
    },
    provenance: {
      resolvedRepositoryUrl: row.resolvedRepositoryUrl ?? row.repositoryUrl ?? null,
      declaredRepositoryUrl: row.declaredRepositoryUrl ?? null,
      mappingConfidenceScore: mappingConfidence,
      mappingConfidenceLabel: repoMappingLabel(mappingConfidence),
      lastCommitAt: toIsoOrNull(row.repoLastCommitAt),
    },
    supplyChainIntegrity: {
      buildProvenance: null,
      signedReleases: null,
    },
  }
}
