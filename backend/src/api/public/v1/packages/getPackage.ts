import type { Request, Response } from 'express'
import { z } from 'zod'

import { NotFoundError } from '@crowd/common'
import { getAdvisoriesByPackageId, getPackageDetailByPurl } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import type { StewardshipStatus } from './types'

const querySchema = z.object({
  purl: z
    .string()
    .trim()
    .min(1)
    .refine((v) => v.startsWith('pkg:'), { message: 'purl must start with pkg:' }),
})

export async function getPackage(req: Request, res: Response): Promise<void> {
  const { purl } = validateOrThrow(querySchema, req.query)

  const qx = await getPackagesQx()
  const pkg = await getPackageDetailByPurl(qx, purl)

  if (!pkg) {
    throw new NotFoundError()
  }

  const advisories = await getAdvisoriesByPackageId(qx, pkg.id)

  ok(res, {
    purl: pkg.purl,
    name: pkg.name,
    ecosystem: pkg.ecosystem,
    general: {
      healthScore: null,
      impact: {
        impactScore: pkg.criticalityScore != null ? Math.round(Number(pkg.criticalityScore)) : null,
        downloadsLastMonth:
          pkg.downloadsLast30d != null ? parseInt(pkg.downloadsLast30d, 10) : null,
        dependentPackages: pkg.dependentPackagesCount ?? null,
        dependentRepos: pkg.dependentReposCount ?? null,
        transitiveReach: null,
      },
      riskSignals: {
        lifecycle: null,
        maintainerBusFactor: null,
        lastRelease: pkg.latestReleaseAt ? pkg.latestReleaseAt.toISOString() : null,
        hasSecurityFile: pkg.hasSecurityFile ?? null,
        openSSFScorecard: pkg.scorecardScore != null ? Number(pkg.scorecardScore) : null,
      },
    },
    assessment: {},
    security: {
      securityContacts: null,
      advisories: advisories.map((a) => ({
        osvId: a.osvId,
        severity: a.severity,
        resolution: null,
      })),
      cvd: {
        isPvrEnabled: null,
        hasSecurityPolicyEnabled: pkg.hasSecurityPolicy ?? null,
        tier0Steward: null,
        criticalVulnerabilityFlag: pkg.hasCriticalVulnerability,
      },
    },
    provenance: {
      repositoryMapping: {
        declaredRepo: pkg.repoUrl ?? pkg.repositoryUrl ?? pkg.declaredRepositoryUrl ?? null,
        mappingConfidence:
          pkg.repoMappingConfidence != null ? Number(pkg.repoMappingConfidence) : null,
        lastCommitAt: pkg.repoLastCommitAt ? pkg.repoLastCommitAt.toISOString() : null,
      },
      supplyChainIntegrity: {
        buildProvenance: null,
        signedReleases: null,
      },
    },
    stewardship: {
      status: (pkg.stewardshipStatus ?? 'unassigned') as StewardshipStatus,
      stewards: null,
      lastActivityAt: null,
    },
    history: {},
  })
}
