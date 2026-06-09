import type { Request, Response } from 'express'
import { z } from 'zod'

import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

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

  const packages: Record<string, object> = {}
  for (const purl of purls) {
    const name = purl.split('/').pop()?.split('@')[0] ?? purl
    const ecosystem = purl.startsWith('pkg:npm') ? 'npm' : purl.startsWith('pkg:maven') ? 'maven' : 'unknown'
    packages[purl] = {
      name,
      ecosystem,
      lifecycle: null,
      health: null,
      impact: null,
      openVulns: null,
      status: 'unassigned',
      origin: 'auto_imported',
      stewards: [],
      lastActivityAt: null,
      lastActivityDescription: null,
    }
  }

  ok(res, { packages })
}
