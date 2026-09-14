import type { Request, Response } from 'express'
import { z } from 'zod'

import { NotFoundError } from '@crowd/common'
import { ESCALATION_RESOLUTION_PATHS, escalateStewardship } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { actorInputSchema } from './actorSchema'

const paramsSchema = z.object({
  id: z.coerce.number().int().positive(),
})

const bodySchema = z.object({
  resolutionPath: z.enum(ESCALATION_RESOLUTION_PATHS),
  notes: z.string().trim().min(1).optional(),
  actor: actorInputSchema,
})

export async function escalateHandler(req: Request, res: Response): Promise<void> {
  const { id } = validateOrThrow(paramsSchema, req.params)
  const { resolutionPath, notes, actor } = validateOrThrow(bodySchema, req.body)

  const qx = await getPackagesQx()
  const stewardship = await escalateStewardship(qx, id, {
    resolutionPath,
    notes,
    actorUserId: req.actor.id,
    actorUsername: actor.username ?? null,
    actorDisplayName: actor.displayName ?? null,
    actorAvatarUrl: actor.avatarUrl ?? null,
  })

  if (!stewardship) {
    throw new NotFoundError(`Stewardship not found: ${id}`)
  }

  ok(res, { stewardship })
}
