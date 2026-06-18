import type { Request, Response } from 'express'
import { z } from 'zod'

import { NotFoundError } from '@crowd/common'
import { openStewardshipByPurl } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { purlFieldSchema } from '../packages/purl'

const bodySchema = z.object({
  purl: purlFieldSchema,
})

export async function openStewardship(req: Request, res: Response): Promise<void> {
  const { purl } = validateOrThrow(bodySchema, req.body)

  const qx = await getPackagesQx()
  const stewardship = await openStewardshipByPurl(qx, purl, req.actor.id)

  if (!stewardship) {
    throw new NotFoundError(`Package not found: ${purl}`)
  }

  ok(res, { stewardship })
}
