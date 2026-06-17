import type { Request, Response } from 'express'

import { NotFoundError } from '@crowd/common'
import { getAdvisoriesByPackageId, getPackageDetailByPurl } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { purlQuerySchema } from './purl'

export async function getPackageAdvisories(req: Request, res: Response): Promise<void> {
  const { purl } = validateOrThrow(purlQuerySchema, req.query)

  const qx = await getPackagesQx()
  const pkg = await getPackageDetailByPurl(qx, purl)

  if (!pkg) {
    throw new NotFoundError()
  }

  const advisories = await getAdvisoriesByPackageId(qx, pkg.id)

  ok(res, {
    advisories: advisories.map((a) => ({
      osvId: a.osvId,
      severity: a.severity,
      resolution: a.resolution,
    })),
    total: advisories.length,
  })
}
