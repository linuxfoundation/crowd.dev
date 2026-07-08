import type { Request, Response } from 'express'
import { z } from 'zod'

import { NotFoundError } from '@crowd/common'
import { openStewardshipByPurl } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { purlFieldSchema } from '../packages/purl'

import { actorInputSchema } from './actorSchema'

const bodySchema = z.object({
  purl: purlFieldSchema,
  actor: actorInputSchema,
})

export async function openStewardship(req: Request, res: Response): Promise<void> {
  const { purl, actor } = validateOrThrow(bodySchema, req.body)

  const qx = await getPackagesQx()
  const stewardship = await openStewardshipByPurl(
    qx,
    purl,
    req.actor.id,
    actor.username ?? null,
    actor.displayName ?? null,
    actor.avatarUrl ?? null,
  )

  if (!stewardship) {
    throw new NotFoundError(`Package not found: ${purl}`)
  }

  ok(res, { stewardship })
}
