import type { Request, Response } from 'express'

import { NotFoundError } from '@crowd/common'
import { getPackageDetailByPurl, listPackageHistory } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { purlQuerySchema } from './purl'

export async function getPackageHistory(req: Request, res: Response): Promise<void> {
  const { purl } = validateOrThrow(purlQuerySchema, req.query)

  const qx = await getPackagesQx()
  const pkg = await getPackageDetailByPurl(qx, purl)

  if (!pkg) {
    throw new NotFoundError()
  }

  if (!pkg.stewardshipId) {
    ok(res, { events: [], total: 0 })
    return
  }

  const events = await listPackageHistory(qx, pkg.stewardshipId)

  ok(res, { events, total: events.length })
}
