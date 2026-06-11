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
    .array(
      z
        .string()
        .trim()
        .min(1)
        .refine((v) => v.startsWith('pkg:'), { message: 'each purl must start with pkg:' }),
    )
    .min(1)
    .max(MAX_PURLS, `Maximum ${MAX_PURLS} purls per request`),
})

export async function batchGetStewardship(req: Request, res: Response): Promise<void> {
  const { purls: rawPurls } = validateOrThrow(bodySchema, req.body)
  const normalizedPurls = rawPurls.map((p) => p.replace(/@/g, '%40'))

  const qx = await getPackagesQx()
  const rows = await getPackagesByStewardshipPurls(qx, normalizedPurls)

  const byPurl = new Map(rows.map((r) => [r.purl, r]))

  const packages: Record<string, StewardshipSummary | null> = {}
  for (let i = 0; i < rawPurls.length; i++) {
    const row = byPurl.get(normalizedPurls[i])
    if (!row) {
      packages[rawPurls[i]] = null
    } else {
      packages[rawPurls[i]] = {
        name: row.name,
        ecosystem: row.ecosystem,
        lifecycle: null,
        health: null,
        impact:
          row.criticalityScore != null ? Math.round(Number(row.criticalityScore) * 100) : null,
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
