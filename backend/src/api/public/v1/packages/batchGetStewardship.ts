import type { Request, Response } from 'express'
import { z } from 'zod'

import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { extractEcosystem, extractName } from './purl'

const MAX_PURLS = 100

const bodySchema = z.object({
  purls: z
    .array(z.string().trim().min(1))
    .min(1)
    .max(MAX_PURLS, `Maximum ${MAX_PURLS} purls per request`),
})

interface StewardshipSummary {
  name: string
  ecosystem: string
  lifecycle: string | null
  health: number | null
  impact: number | null
  openVulns: { low: number; medium: number; high: number; critical: number } | null
  stewardship: string
  stewards: null
  lastActivityAt: string | null
  lastActivityDescription: string | null
}

// TODO: replace with real DB queries once stewardship tables land
export async function batchGetStewardship(req: Request, res: Response): Promise<void> {
  const { purls } = validateOrThrow(bodySchema, req.body)

  const packages: Record<string, StewardshipSummary | null> = {}
  for (const purl of purls) {
    if (!purl.startsWith('pkg:')) {
      packages[purl] = null
    } else {
      packages[purl] = {
        name: extractName(purl),
        ecosystem: extractEcosystem(purl),
        lifecycle: null,
        health: null,
        impact: null,
        openVulns: null,
        stewardship: 'unassigned',
        stewards: null,
        lastActivityAt: null,
        lastActivityDescription: null,
      }
    }
  }

  ok(res, { packages })
}
