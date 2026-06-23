import type { Request, Response } from 'express'

import { NotFoundError } from '@crowd/common'
import {
  computeHealthBand,
  getAdvisoriesByPackageId,
  getPackageDetailByPurl,
  getStewardshipSummary,
} from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { purlQuerySchema } from './purl'
import type { StewardshipStatus } from './types'

function repoMappingLabel(confidence: number | null): 'High' | 'Medium' | 'Low' | null {
  if (confidence === null) return null
  if (confidence >= 0.8) return 'High'
  if (confidence >= 0.5) return 'Medium'
  return 'Low'
}

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
  const mappingConfidence =
    pkg.repoMappingConfidence != null ? Number(pkg.repoMappingConfidence) : null

  ok(res, {
    purl: pkg.purl,
    name: pkg.name,
    ecosystem: pkg.ecosystem,
    latestVersion: pkg.latestVersion ?? null,
    general: {
      healthScore: scorecardScore !== null ? Math.round(scorecardScore * 10) : null,
      healthBand: computeHealthBand(scorecardScore),
      impact: {
        impactScore:
          pkg.criticalityScore != null ? Math.round(Number(pkg.criticalityScore) * 100) : null,
        downloadsLastMonth: pkg.downloadsLast30d ?? null,
        dependentPackages: pkg.dependentPackagesCount ?? null,
        dependentRepos: pkg.dependentReposCount ?? null,
        transitiveReach: pkg.transitiveReach,
      },
      riskSignals: {
        lifecycle: null,
        maintainerBusFactor: pkg.maintainerCount,
        lastRelease: pkg.latestReleaseAt ? pkg.latestReleaseAt.toISOString() : null,
        hasSecurityFile: pkg.hasSecurityFile,
        hasSecurityPolicy: pkg.hasSecurityPolicy,
        branchProtectionEnabled: pkg.branchProtectionEnabled,
        openSSFScorecard: scorecardScore,
      },
    },
    assessment: null,
    security: {
      securityContacts: null,
      advisories: advisories.map((a) => ({
        osvId: a.osvId,
        severity: a.severity,
        resolution: a.resolution,
      })),
      cvd: {
        isPvrEnabled: null,
        tier0Steward: null,
        criticalVulnerabilityFlag: pkg.hasCriticalVulnerability,
      },
    },
    provenance: {
      repositoryMapping: {
        declaredRepo: pkg.repoUrl ?? pkg.repositoryUrl ?? pkg.declaredRepositoryUrl ?? null,
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
