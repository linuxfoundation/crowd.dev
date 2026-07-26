import type { Request, Response } from 'express'

import { NotFoundError } from '@crowd/common'
import {
  computeHealthBand,
  getAdvisoriesByPackageId,
  getPackageDetailByPurl,
  getStewardshipSummary,
  securityContactConfidenceBand,
} from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { repoMappingLabel, snakeToCamelKeys, toNullableNumber } from './mappers'
import { purlQuerySchema } from './purl'
import { HEALTH_BAND_SET, LIFECYCLE_VALUES, type StewardshipStatus } from './types'

const LIFECYCLE_SET = new Set<string>(LIFECYCLE_VALUES)

export async function getPackage(req: Request, res: Response): Promise<void> {
  const { purl } = validateOrThrow(purlQuerySchema, req.query)

  const qx = await getPackagesQx()
  const pkg = await getPackageDetailByPurl(qx, purl)

  if (!pkg) {
    throw new NotFoundError()
  }

  const [{ rows: advisories }, stewardshipSummary] = await Promise.all([
    getAdvisoriesByPackageId(qx, pkg.id),
    pkg.stewardshipId ? getStewardshipSummary(qx, Number(pkg.stewardshipId)) : null,
  ])

  const scorecardScore = pkg.scorecardScore != null ? Number(pkg.scorecardScore) : null
  const mappingConfidence = toNullableNumber(pkg.repoMappingConfidence)

  const securityContacts =
    pkg.contactsLastRefreshed == null
      ? null
      : (pkg.securityContacts ?? []).map((c) => ({
          channel: c.channel,
          value: c.value,
          role: c.role,
          confidence: c.confidence,
          score: Number(c.score),
        }))
  const packageConfidence =
    securityContacts && securityContacts.length > 0
      ? securityContactConfidenceBand(Math.max(...securityContacts.map((c) => c.score)))
      : null

  ok(res, {
    purl: pkg.purl,
    name: pkg.name,
    ecosystem: pkg.ecosystem,
    latestVersion: pkg.latestVersion ?? null,
    general: {
      healthScore: pkg.healthScore,
      healthScoreDetails: {
        total: pkg.healthScore,
        label:
          pkg.healthLabel != null && HEALTH_BAND_SET.has(pkg.healthLabel) ? pkg.healthLabel : null,
        maintainerHealth: pkg.maintainerHealthScore,
        securitySupplyChain: pkg.securitySupplyChainScore,
        developmentActivity: pkg.developmentActivityScore,
      },
      healthBand:
        pkg.healthLabel != null && HEALTH_BAND_SET.has(pkg.healthLabel)
          ? pkg.healthLabel
          : computeHealthBand(scorecardScore),
      impact: {
        impactScore: pkg.criticalityScore != null ? Math.round(pkg.criticalityScore * 100) : null,
        downloadsLastMonth: pkg.downloadsLast30d ?? null,
        dependentPackages: pkg.dependentPackagesCount ?? null,
        dependentRepos: pkg.dependentReposCount ?? null,
        transitiveReach: pkg.transitiveReach,
      },
      riskSignals: {
        lifecycle:
          pkg.lifecycleLabel != null && LIFECYCLE_SET.has(pkg.lifecycleLabel)
            ? pkg.lifecycleLabel
            : null,
        maintainerBusFactor: pkg.maintainerCount,
        lastRelease: pkg.latestReleaseAt ? pkg.latestReleaseAt.toISOString() : null,
        hasSecurityFile: pkg.hasSecurityFile,
        hasSecurityPolicy: pkg.hasSecurityPolicy,
        branchProtectionEnabled: pkg.branchProtectionEnabled,
        openSSFScorecard: scorecardScore,
      },
    },
    signalCoverageHealth: snakeToCamelKeys(pkg.signalCoverageHealth),
    assessment: null,
    security: {
      securityContacts,
      packageConfidence,
      securityPolicies: {
        securityPolicyUrl: pkg.securityPolicyUrl ?? null,
        vulnerabilityReportingUrl: pkg.vulnerabilityReportingUrl ?? null,
        bugBountyUrl: pkg.bugBountyUrl ?? null,
        pvrEnabled: pkg.pvrEnabled ?? null,
      },
      advisories: advisories.map((a) => ({
        osvId: a.osvId,
        severity: a.severity,
        resolution: a.resolution,
        isCritical: a.isCritical,
      })),
      cvd: {
        isPvrEnabled: pkg.pvrEnabled ?? null,
        tier0Steward: null,
        criticalVulnerabilityFlag: pkg.hasCriticalVulnerability,
      },
    },
    provenance: {
      repositoryMapping: {
        declaredRepo: pkg.repoUrl ?? pkg.repositoryUrl ?? null,
        mappingConfidence,
        mappingLabel: repoMappingLabel(mappingConfidence),
        lastCommitAt: pkg.repoLastCommitAt ? pkg.repoLastCommitAt.toISOString() : null,
      },
      supplyChainIntegrity: {
        buildProvenance: null,
        signedReleases: null,
      },
    },
    stewardship: {
      id: pkg.stewardshipId ?? null,
      status: (pkg.stewardshipStatus ?? 'unassigned') as StewardshipStatus,
      origin: pkg.stewardshipOrigin ?? null,
      version: pkg.stewardshipVersion ?? null,
      openedAt: pkg.stewardshipOpenedAt ? pkg.stewardshipOpenedAt.toISOString() : null,
      lastStatusAt: pkg.stewardshipLastStatusAt ? pkg.stewardshipLastStatusAt.toISOString() : null,
      resolutionPath: pkg.stewardshipResolutionPath ?? null,
      statusNote: pkg.stewardshipStatusNote ?? null,
      stewards: stewardshipSummary?.stewards ?? null,
      lastActivityAt: stewardshipSummary?.lastActivityAt ?? null,
    },
    history: null,
  })
}
