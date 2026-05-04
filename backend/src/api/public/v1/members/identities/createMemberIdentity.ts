import type { Request, Response } from 'express'
import { z } from 'zod'

import { captureApiChange, memberEditIdentitiesAction } from '@crowd/audit-logs'
import { ConflictError, NotFoundError } from '@crowd/common'
import {
  MemberField,
  findMemberById,
  createMemberIdentity as insertMemberIdentity,
  optionsQx,
  touchMemberUpdatedAt,
} from '@crowd/data-access-layer'
import { IMemberIdentity, MemberIdentityType } from '@crowd/types'

import { created } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

const paramsSchema = z.object({
  memberId: z.uuid(),
})

const bodySchema = z
  .object({
    value: z.string().min(1),
    platform: z.string().min(1),
    type: z.enum(MemberIdentityType),
    source: z.string().min(1),
    verified: z.boolean(),
    verifiedBy: z.string().optional(),
  })
  .refine((data) => !data.verified || data.verifiedBy, {
    message: 'verifiedBy is required when verified is true',
    path: ['verifiedBy'],
  })

export async function createMemberIdentity(req: Request, res: Response): Promise<void> {
  const { memberId } = validateOrThrow(paramsSchema, req.params)
  const data = validateOrThrow(bodySchema, req.body)

  const qx = optionsQx(req)

  const member = await findMemberById(qx, memberId, [MemberField.ID])
  if (!member) {
    throw new NotFoundError('Member not found')
  }

  let result!: IMemberIdentity

  await captureApiChange(
    req,
    memberEditIdentitiesAction(memberId, async (captureOldState, captureNewState) => {
      captureOldState({})

      await qx.tx(async (tx) => {
        try {
          result = await insertMemberIdentity(
            tx,
            {
              memberId,
              platform: data.platform,
              value: data.value,
              type: data.type,
              source: data.source,
              verified: data.verified,
              verifiedBy: data.verifiedBy,
            },
            true,
            true,
          )
        } catch (error) {
          const constraint =
            error.constraint ?? error.original?.constraint ?? error.parent?.constraint

          if (constraint === 'uix_memberIdentities_memberId_platform_value_type') {
            throw new ConflictError('Identity already exists on this member')
          }

          if (constraint === 'uix_memberIdentities_platform_value_type_verified') {
            throw new ConflictError('Identity already verified on another member')
          }

          throw error
        }

        // touch member updated at to trigger merge suggestion
        await touchMemberUpdatedAt(tx, memberId)
      })

      captureNewState(result)
    }),
  )

  created(res, {
    id: result.id,
    value: result.value,
    platform: result.platform,
    verified: result.verified,
    verifiedBy: result.verifiedBy ?? null,
    source: result.source ?? null,
    createdAt: result.createdAt,
    updatedAt: result.updatedAt,
  })
}
