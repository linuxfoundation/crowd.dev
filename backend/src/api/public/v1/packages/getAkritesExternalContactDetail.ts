import type { Request, Response } from 'express'

import { NotFoundError } from '@crowd/common'
import { getContactDetailsByPurls } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { toAkritesExternalContactDetail } from './akritesExternalContactDetail'
import { purlQuerySchema } from './purl'

export async function getAkritesExternalContactDetail(req: Request, res: Response): Promise<void> {
  const { purl } = validateOrThrow(purlQuerySchema, req.query)

  const qx = await getPackagesQx()
  const [row] = await getContactDetailsByPurls(qx, [purl])

  if (!row) {
    throw new NotFoundError()
  }

  ok(res, toAkritesExternalContactDetail(row))
}
