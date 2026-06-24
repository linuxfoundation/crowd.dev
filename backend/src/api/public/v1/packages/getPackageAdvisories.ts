import type { Request, Response } from 'express'
import { z } from 'zod'

import { NotFoundError } from '@crowd/common'
import { getAdvisoriesByPackageId, getPackageDetailByPurl } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { purlQuerySchema } from './purl'

const DEFAULT_PAGE_SIZE = 20
const MAX_PAGE_SIZE = 100

const SEVERITY_VALUES = ['critical', 'high', 'moderate', 'low'] as const
const RESOLUTION_VALUES = ['open', 'patched'] as const

function toStringArray(v: unknown): unknown {
  if (!v) return undefined
  const vals = Array.isArray(v) ? v : [v]
  return vals
    .flatMap((s: unknown) => String(s).split(','))
    .map((s) => s.trim())
    .filter(Boolean)
}

const querySchema = purlQuerySchema.extend({
  page: z.coerce.number().int().min(1).default(1),
  pageSize: z.coerce.number().int().min(1).max(MAX_PAGE_SIZE).default(DEFAULT_PAGE_SIZE),
  severity: z.preprocess(toStringArray, z.array(z.enum(SEVERITY_VALUES)).optional()),
  resolution: z.preprocess(toStringArray, z.array(z.enum(RESOLUTION_VALUES)).optional()),
  critical: z
    .preprocess((v) => {
      if (v === 'true') return true
      if (v === 'false') return false
      return v
    }, z.boolean().optional())
    .optional(),
})

export async function getPackageAdvisories(req: Request, res: Response): Promise<void> {
  const { purl, page, pageSize, severity, resolution, critical } = validateOrThrow(
    querySchema,
    req.query,
  )

  const qx = await getPackagesQx()
  const pkg = await getPackageDetailByPurl(qx, purl)

  if (!pkg) {
    throw new NotFoundError()
  }

  const { rows, total } = await getAdvisoriesByPackageId(qx, pkg.id, {
    page,
    pageSize,
    severities: severity,
    resolutions: resolution,
    critical,
  })

  ok(res, {
    page,
    pageSize,
    total,
    advisories: rows.map((a) => ({
      osvId: a.osvId,
      severity: a.severity,
      resolution: a.resolution,
      isCritical: a.isCritical,
    })),
  })
}
