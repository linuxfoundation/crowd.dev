import type { Request, Response } from 'express'
import { z } from 'zod'

import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { MOCK_DETAILS } from './mockData'

const MAX_PURLS = 100

const bodySchema = z.object({
  purls: z
    .array(z.string().trim().min(1))
    .min(1)
    .max(MAX_PURLS, `Maximum ${MAX_PURLS} purls per request`),
})

// TODO: replace with real DB queries once stewardship tables land
export async function batchGetStewardship(req: Request, res: Response): Promise<void> {
  const { purls } = validateOrThrow(bodySchema, req.body)

  const packages: Record<string, object | null> = {}
  for (const purl of purls) {
    const detail = MOCK_DETAILS[purl]
    if (!detail) {
      packages[purl] = null
    } else {
      const { openVulns } = detail.security.advisories.reduce(
        (acc, a) => {
          acc.openVulns[a.severity as keyof typeof acc.openVulns] =
            (acc.openVulns[a.severity as keyof typeof acc.openVulns] ?? 0) + 1
          return acc
        },
        { openVulns: { low: 0, medium: 0, high: 0, critical: 0 } },
      )
      packages[purl] = {
        name: detail.name,
        ecosystem: detail.ecosystem,
        lifecycle: detail.general.riskSignals.lifecycle,
        health: detail.general.healthScore.total,
        impact: detail.general.impact.impactScore,
        openVulns,
        stewardship: 'unassigned',
        stewards: null,
        lastActivityAt: null,
        lastActivityDescription: null,
      }
    }
  }

  ok(res, { packages })
}
