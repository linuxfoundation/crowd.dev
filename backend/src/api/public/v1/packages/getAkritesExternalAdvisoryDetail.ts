import type { Request, Response } from 'express'

import { NotFoundError } from '@crowd/common'
import { getAdvisoriesByPurls } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { toAkritesExternalAdvisoryDetail } from './akritesExternalAdvisoryDetail'
import { purlQuerySchema } from './purl'

export async function getAkritesExternalAdvisoryDetail(req: Request, res: Response): Promise<void> {
  const { purl } = validateOrThrow(purlQuerySchema, req.query)

  const qx = await getPackagesQx()
  const rows = await getAdvisoriesByPurls(qx, [purl])

  // No rows at all means the package itself doesn't exist. A package that exists but
  // has no advisories still yields one (null-osvId) sentinel row, so it 200s with [].
  if (rows.length === 0) {
    throw new NotFoundError()
  }

  ok(res, toAkritesExternalAdvisoryDetail(purl, rows))
}
