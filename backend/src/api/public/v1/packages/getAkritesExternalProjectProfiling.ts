import type { Request, Response } from 'express'

import { NotFoundError } from '@crowd/common'
import { getReportingProtocolByPurl } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { toAkritesExternalProjectProfiling } from './akritesExternalProjectProfiling'
import { purlQuerySchema } from './purl'

export async function getAkritesExternalProjectProfiling(
  req: Request,
  res: Response,
): Promise<void> {
  const { purl } = validateOrThrow(purlQuerySchema, req.query)

  const qx = await getPackagesQx()
  const row = await getReportingProtocolByPurl(qx, purl)

  if (!row) {
    throw new NotFoundError()
  }

  ok(res, toAkritesExternalProjectProfiling(row))
}
