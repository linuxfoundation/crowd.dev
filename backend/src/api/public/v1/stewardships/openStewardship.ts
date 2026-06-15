import type { Request, Response } from 'express'
import { z } from 'zod'

import { NotFoundError } from '@crowd/common'
import { openStewardshipByPurl } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { normalizePurl } from '../packages/purl'

const bodySchema = z.object({
  purl: z
    .string()
    .trim()
    .min(1)
    .refine((v) => v.startsWith('pkg:'), { message: 'purl must start with pkg:' }),
})

export async function openStewardship(req: Request, res: Response): Promise<void> {
  const { purl: rawPurl } = validateOrThrow(bodySchema, req.body)
  const purl = normalizePurl(rawPurl)

  const qx = await getPackagesQx()
  const stewardship = await openStewardshipByPurl(qx, purl, req.actor.id)

  if (!stewardship) {
    throw new NotFoundError(`Package not found: ${rawPurl}`)
  }

  ok(res, { stewardship })
}
