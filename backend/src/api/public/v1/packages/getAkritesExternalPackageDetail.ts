import type { Request, Response } from 'express'

import { NotFoundError } from '@crowd/common'
import { getPackageDetailsByPurls } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { toAkritesExternalPackageDetail } from './akritesExternalPackageDetail'
import { purlQuerySchema } from './purl'

export async function getAkritesExternalPackageDetail(req: Request, res: Response): Promise<void> {
  const { purl } = validateOrThrow(purlQuerySchema, req.query)

  const qx = await getPackagesQx()
  const [row] = await getPackageDetailsByPurls(qx, [purl])

  if (!row) {
    throw new NotFoundError()
  }

  ok(res, toAkritesExternalPackageDetail(row))
}
