import type { Request, Response } from 'express'
import { z } from 'zod'

import { NotFoundError } from '@crowd/common'
import { ESCALATION_RESOLUTION_PATHS, escalateStewardship } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { stewardshipIdParamsSchema } from './schemas'

const bodySchema = z.object({
  resolutionPath: z.enum(ESCALATION_RESOLUTION_PATHS),
  notes: z.string().trim().min(1).optional(),
})

export async function escalateHandler(req: Request, res: Response): Promise<void> {
  const { id } = validateOrThrow(stewardshipIdParamsSchema, req.params)
  const { resolutionPath, notes } = validateOrThrow(bodySchema, req.body)

  const qx = await getPackagesQx()
  const stewardship = await escalateStewardship(qx, id, {
    resolutionPath,
    notes,
    actorUserId: req.actor.id,
  })

  if (!stewardship) {
    throw new NotFoundError(`Stewardship not found: ${id}`)
  }

  ok(res, { stewardship })
}
