import {
  type AkritesExternalPackageDetailRow,
  type HealthBand,
  computeHealthBand,
} from '@crowd/data-access-layer'

import { repoMappingLabel, snakeToCamelKeys } from './mappers'
import { HEALTH_BAND_SET, LIFECYCLE_VALUES, type Lifecycle } from './types'

const LIFECYCLE_SET = new Set<string>(LIFECYCLE_VALUES)

export type AkritesExternalHealthBand = 'critical' | 'low' | 'medium' | 'high'
export type AkritesExternalLifecycle = 'active' | 'inactive' | 'deprecated' | 'abandoned' | null

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

// UNCONFIRMED CROSSWALK — see the HealthBand schema note in the akrites-external
// OpenAPI contract: the internal excellent/healthy/fair/concerning/critical scale
// (good→bad) has no agreed mapping to Akrites' critical/low/medium/high scale yet.
// This mirrors the "fair -> medium" guess from the original Akrites draft spec
// (not encoded in this repo's openapi.yaml). Confirm the real crosswalk with
// Akrites/product before this ships, then update this table only.
const HEALTH_BAND_CROSSWALK: Record<HealthBand, AkritesExternalHealthBand> = {
  excellent: 'high',
  healthy: 'high',
  fair: 'medium',
  concerning: 'low',
  critical: 'critical',
}

function toAkritesHealthBand(
  healthLabel: string | null,
  scorecardScore: number | null,
): AkritesExternalHealthBand {
  // Same validity guard as getPackage.ts: an unrecognized (not just null) stored
  // label falls back to computeHealthBand() instead of silently miscategorizing.
  const label: HealthBand =
    healthLabel != null && HEALTH_BAND_SET.has(healthLabel)
      ? (healthLabel as HealthBand)
      : computeHealthBand(scorecardScore)
  return HEALTH_BAND_CROSSWALK[label]
}

// UNCONFIRMED CROSSWALK — same caveat as HEALTH_BAND_CROSSWALK above: the internal
// lifecycle scale (active/stable/declining/abandoned/archived) doesn't line up
// one-to-one with Akrites' (active/inactive/deprecated/abandoned/null). Confirm
// with Akrites/product before this ships.
const LIFECYCLE_CROSSWALK: Record<Lifecycle, AkritesExternalLifecycle> = {
  active: 'active',
  stable: 'active',
  declining: 'inactive',
  abandoned: 'abandoned',
  archived: 'deprecated',
}

function toAkritesLifecycle(lifecycleLabel: string | null): AkritesExternalLifecycle {
  if (lifecycleLabel === null || !LIFECYCLE_SET.has(lifecycleLabel)) return null
  return LIFECYCLE_CROSSWALK[lifecycleLabel as Lifecycle]
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
  const mappingConfidence =
    row.repoMappingConfidence != null ? Number(row.repoMappingConfidence) : null

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
