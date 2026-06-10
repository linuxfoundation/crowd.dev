import type { Request, Response } from 'express'
import { z } from 'zod'

import { NotFoundError } from '@crowd/common'

import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

const paramsSchema = z.object({
  purl: z.string().trim().min(1),
})

// TODO: replace with real DB queries once packages DB is wired into the backend
export async function getPackage(req: Request, res: Response): Promise<void> {
  const { purl: rawPurl } = validateOrThrow(paramsSchema, req.params)

  let purl: string
  try {
    purl = decodeURIComponent(rawPurl)
  } catch {
    throw new NotFoundError()
  }

  if (!purl.startsWith('pkg:')) {
    throw new NotFoundError()
  }

  ok(res, mockPackage(purl))
}

function mockPackage(purl: string) {
  const name = purl.split('/').pop()?.split('@')[0] ?? purl
  let ecosystem = 'unknown'
  if (purl.startsWith('pkg:npm')) ecosystem = 'npm'
  else if (purl.startsWith('pkg:maven')) ecosystem = 'maven'

  return {
    purl,
    name,
    ecosystem,
    general: {
      healthScore: {
        maintainerHealth: 4,
        securitySupplyChain: 8,
        developmentActivity: 6,
        total: 18,
      },
      impact: {
        impactScore: 71,
        downloadsLastMonth: ecosystem === 'maven' ? null : 52142891,
        dependentPackages: 142312,
        dependentRepos: 39104,
        transitiveReach: 'Top 0.4%',
      },
      riskSignals: {
        lifecycle: 'declining',
        maintainerBusFactor: 1,
        lastRelease: '2021-02-20T00:00:00Z',
        hasSecurityFile: null,
        openSSFScorecard: 5.2,
      },
    },
    assessment: {},
    security: {
      securityContacts: null,
      advisories: [
        {
          osvId: 'CVE-2021-44906',
          severity: 'high',
          resolution: null,
        },
      ],
      cvd: {
        isPvrEnabled: null,
        hasSecurityPolicyEnabled: null,
        tier0Steward: null,
        criticalVulnerabilityFlag: true,
      },
    },
    provenance: {
      repositoryMapping: {
        declaredRepo: `https://github.com/example/${name}`,
        mappingConfidence: 0.98,
        lastCommitAt: '2024-09-14T00:00:00Z',
      },
      supplyChainIntegrity: {
        buildProvenance: null,
        signedReleases: null,
      },
    },
    history: {},
  }
}
