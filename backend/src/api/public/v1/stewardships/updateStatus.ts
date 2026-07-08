import type { Request, Response } from 'express'
import { z } from 'zod'

import { NotFoundError } from '@crowd/common'
import {
  INACTIVE_REASONS,
  STEWARDSHIP_UPDATABLE_STATUSES,
  updateStewardshipStatus,
} from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { actorInputSchema } from './actorSchema'

const paramsSchema = z.object({
  id: z.coerce.number().int().positive(),
})

const bodySchema = z
  .object({
    status: z.enum(STEWARDSHIP_UPDATABLE_STATUSES),
    inactiveReason: z.enum(INACTIVE_REASONS).optional(),
    notes: z.string().trim().min(1).optional(),
    actor: actorInputSchema,
  })
  .refine((d) => d.status !== 'inactive' || !!d.inactiveReason, {
    message: 'inactiveReason is required when status is inactive',
    path: ['inactiveReason'],
  })

export async function updateStatusHandler(req: Request, res: Response): Promise<void> {
  const { id } = validateOrThrow(paramsSchema, req.params)
  const { status, inactiveReason, notes, actor } = validateOrThrow(bodySchema, req.body)

  const qx = await getPackagesQx()
  const stewardship = await updateStewardshipStatus(qx, id, {
    status,
    inactiveReason,
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
