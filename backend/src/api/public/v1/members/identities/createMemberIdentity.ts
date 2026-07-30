import type { Request, Response } from 'express'
import { z } from 'zod'

import { captureApiChange, memberEditIdentitiesAction } from '@crowd/audit-logs'
import { NotFoundError } from '@crowd/common'
import {
  MemberField,
  findMemberById,
  findMemberIdentitiesByValue,
  insertMemberIdentities,
  touchMemberUpdatedAt,
  updateMemberIdentity,
} from '@crowd/data-access-layer'
import { IMemberIdentity, MemberIdentityType } from '@crowd/types'

import { rethrowIdentityConflict } from '@/api/public/alerts/identityConflict'
import { optionsQx } from '@/database/sequelizeQueryExecutor'
import { created, ok } from '@/utils/api'
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
  let alreadyExisted = false

  await captureApiChange(
    req,
    memberEditIdentitiesAction(memberId, async (captureOldState, captureNewState) => {
      captureOldState({})

      await qx.tx(async (tx) => {
        const existing = await findMemberIdentitiesByValue(tx, memberId, data.value, {
          type: data.type,
        })
        const exactMatch = existing.find((i) => i.platform === data.platform)

        try {
          if (exactMatch) {
            alreadyExisted = true
            result = exactMatch
          } else {
            const [created] = await insertMemberIdentities(
              tx,
              [
                {
                  memberId,
                  platform: data.platform,
                  value: data.value,
                  type: data.type,
                  source: data.source,
                  verified: data.verified,
                  verifiedBy: data.verifiedBy,
                },
              ],
              true,
              true,
            )
            result = created
          }

          // A verified identity confirms the same value for this member, so keep same-value
          // identities in sync instead of leaving stale unverified duplicates behind.
          if (data.verified && existing.length > 0) {
            const updatedResults: IMemberIdentity[] = []
            for (const identity of existing) {
              const updated = await updateMemberIdentity(tx, memberId, identity.id, {
                verified: true,
                verifiedBy: data.verifiedBy,
              })
              if (updated) updatedResults.push(updated)
            }

            if (alreadyExisted) {
              result = updatedResults.find((r) => r.id === exactMatch.id) ?? result
            }
          }
        } catch (error) {
          rethrowIdentityConflict(req, error, {
            memberId,
            platform: data.platform,
            value: data.value,
            type: data.type,
          })
        }

        await touchMemberUpdatedAt(tx, memberId)
      })

      captureNewState(result)
    }),
  )

  const response = {
    id: result.id,
    value: result.value,
    platform: result.platform,
    type: result.type,
    verified: result.verified,
    verifiedBy: result.verifiedBy ?? null,
    source: result.source ?? null,
    createdAt: result.createdAt,
    updatedAt: result.updatedAt,
  }

  if (alreadyExisted) {
    ok(res, response)
  } else {
    created(res, response)
  }
}
