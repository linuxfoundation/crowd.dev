import type { Request, Response } from 'express'
import { z } from 'zod'

import { NotFoundError } from '@crowd/common'
import { assignSteward } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { actorInputSchema } from './actorSchema'

const paramsSchema = z.object({
  id: z.coerce.number().int().positive(),
})

const bodySchema = z.object({
  steward: z
    .object({
      userId: z.string().trim().min(1),
      username: z.string().trim().min(1).optional().nullable(),
      displayName: z.string().trim().min(1).optional().nullable(),
      role: z.enum(['lead', 'co_steward']),
    })
    .refine((d) => (d.username == null) === (d.displayName == null), {
      message: 'username and displayName must both be provided or both be absent',
      path: ['displayName'],
    }),
  note: z.string().trim().min(1).optional(),
  moveToAssessing: z.boolean().optional().default(false),
  actor: actorInputSchema,
})

export async function assignStewardHandler(req: Request, res: Response): Promise<void> {
  const { id } = validateOrThrow(paramsSchema, req.params)
  const { steward, note, moveToAssessing, actor } = validateOrThrow(bodySchema, req.body)

  const qx = await getPackagesQx()
  const result = await assignSteward(qx, id, {
    userId: steward.userId,
    username: steward.username,
    displayName: steward.displayName,
    role: steward.role,
    note,
    assignedBy: req.actor.id,
    actorUsername: actor.username ?? null,
    actorDisplayName: actor.displayName ?? null,
    actorAvatarUrl: actor.avatarUrl ?? null,
    moveToAssessing,
  })

  if (!result) {
    throw new NotFoundError(`Stewardship not found: ${id}`)
  }

  ok(res, { stewardship: result.stewardship, stewards: result.stewards })
}
