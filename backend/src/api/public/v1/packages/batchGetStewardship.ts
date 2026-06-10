import type { Request, Response } from 'express'
import { z } from 'zod'

import { getPackagesByStewardshipPurls } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import type { StewardshipSummary } from './types'

const MAX_PURLS = 100

const bodySchema = z.object({
  purls: z
    .array(z.string().trim().min(1))
    .min(1)
    .max(MAX_PURLS, `Maximum ${MAX_PURLS} purls per request`),
})

export async function batchGetStewardship(req: Request, res: Response): Promise<void> {
  const { purls } = validateOrThrow(bodySchema, req.body)

  const qx = await getPackagesQx()
  const rows = await getPackagesByStewardshipPurls(qx, purls)

  const byPurl = new Map(rows.map((r) => [r.purl, r]))

  const packages: Record<string, StewardshipSummary | null> = {}
  for (const purl of purls) {
    const row = byPurl.get(purl)
    if (!row) {
      packages[purl] = null
    } else {
      packages[purl] = {
        name: row.name,
        ecosystem: row.ecosystem,
        lifecycle: null,
        health: null,
        impact: row.criticalityScore != null ? Math.round(Number(row.criticalityScore)) : null,
        openVulns: null,
        stewardship: (row.stewardshipStatus ?? 'unassigned') as StewardshipSummary['stewardship'],
        stewards: null,
        lastActivityAt: null,
        lastActivityDescription: null,
      }
    }
  }

  ok(res, { packages })
}
