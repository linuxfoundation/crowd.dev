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

const querySchema = purlQuerySchema.extend({
  page: z.coerce.number().int().min(1).default(1),
  pageSize: z.coerce.number().int().min(1).max(MAX_PAGE_SIZE).default(DEFAULT_PAGE_SIZE),
})

export async function getPackageAdvisories(req: Request, res: Response): Promise<void> {
  const { purl, page, pageSize } = validateOrThrow(querySchema, req.query)

  const qx = await getPackagesQx()
  const pkg = await getPackageDetailByPurl(qx, purl)

  if (!pkg) {
    throw new NotFoundError()
  }

  const { rows, total } = await getAdvisoriesByPackageId(qx, pkg.id, { page, pageSize })

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
