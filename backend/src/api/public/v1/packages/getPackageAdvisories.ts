import type { Request, Response } from 'express'
import { z } from 'zod'

import { NotFoundError } from '@crowd/common'
import { getAdvisoriesByPackageId, getPackageDetailByPurl } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { extractPurlVersion, purlQuerySchema } from './purl'

const DEFAULT_PAGE_SIZE = 20
const MAX_PAGE_SIZE = 100

const SEVERITY_VALUES = ['critical', 'high', 'moderate', 'low'] as const

const querySchema = purlQuerySchema.extend({
  page: z.coerce.number().int().min(1).default(1),
  pageSize: z.coerce.number().int().min(1).max(MAX_PAGE_SIZE).default(DEFAULT_PAGE_SIZE),
  severity: z
    .preprocess(
      (v) => {
        if (!v) return undefined
        const vals = Array.isArray(v) ? v : [v]
        return vals.flatMap((s: unknown) => String(s).split(','))
      },
      z.array(z.enum(SEVERITY_VALUES)).optional(),
    )
    .optional(),
})

export async function getPackageAdvisories(req: Request, res: Response): Promise<void> {
  const rawPurl = typeof req.query.purl === 'string' ? req.query.purl : ''
  const version = extractPurlVersion(rawPurl)

  const { purl, page, pageSize, severity } = validateOrThrow(querySchema, req.query)

  const qx = await getPackagesQx()
  const pkg = await getPackageDetailByPurl(qx, purl)

  if (!pkg) {
    throw new NotFoundError()
  }

  const { rows, total } = await getAdvisoriesByPackageId(qx, pkg.id, {
    page,
    pageSize,
    version: version ?? undefined,
    severities: severity,
  })

  ok(res, {
    page,
    pageSize,
    total,
    advisories: rows.map((a) => ({
      osvId: a.osvId,
      severity: a.severity,
      resolution: a.resolution,
    })),
  })
}
