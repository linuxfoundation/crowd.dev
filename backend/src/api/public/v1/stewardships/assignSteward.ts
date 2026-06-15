import type { Request, Response } from 'express'
import { z } from 'zod'

import { NotFoundError } from '@crowd/common'
import { assignSteward } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

const paramsSchema = z.object({
  id: z.coerce.number().int().positive(),
})

const bodySchema = z.object({
  userId: z.string().trim().min(1),
  role: z.enum(['lead', 'co_steward']),
})

export async function assignStewardHandler(req: Request, res: Response): Promise<void> {
  const { id } = validateOrThrow(paramsSchema, req.params)
  const { userId, role } = validateOrThrow(bodySchema, req.body)

  const qx = await getPackagesQx()
  const result = await assignSteward(qx, id, { userId, role, assignedBy: req.actor.id })

  if (!result) {
    throw new NotFoundError(`Stewardship not found: ${id}`)
  }

  ok(res, { stewardship: result.stewardship, stewards: result.stewards })
}
