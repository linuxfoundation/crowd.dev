import type { Request, Response } from 'express'
import { z } from 'zod'

import { captureApiChange, memberEditIdentitiesAction } from '@crowd/audit-logs'
import { NotFoundError } from '@crowd/common'
import {
  MemberField,
  findMemberById,
  findMemberIdentitiesByValue,
  createMemberIdentity as insertMemberIdentity,
  touchMemberUpdatedAt,
  updateMemberIdentity,
} from '@crowd/data-access-layer'
import { IMemberIdentity, MemberIdentityType } from '@crowd/types'

import { optionsQx } from '@/database/sequelizeQueryExecutor'
import { created, ok } from '@/utils/api'
import { rethrowDbConflict } from '@/utils/err'
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

  // The data-sink writes identity values as trimmed lowercase, so normalize here
  // to keep idempotency checks reliable against existing rows.
  const normalizedValue = data.value.trim().toLowerCase()

  let result!: IMemberIdentity
  let alreadyExisted = false

  await captureApiChange(
    req,
    memberEditIdentitiesAction(memberId, async (captureOldState, captureNewState) => {
      captureOldState({})

      await qx.tx(async (tx) => {
        const existing = await findMemberIdentitiesByValue(tx, memberId, normalizedValue, {
          type: data.type,
        })
        const exactMatch = existing.find((i) => i.platform === data.platform)

        try {
          if (exactMatch) {
            alreadyExisted = true
            result = exactMatch
          } else {
            result = await insertMemberIdentity(
              tx,
              {
                memberId,
                platform: data.platform,
                value: normalizedValue,
                type: data.type,
                source: data.source,
                verified: data.verified,
                verifiedBy: data.verifiedBy,
              },
              true,
              true,
            )
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
          const ctx = { platform: data.platform, value: normalizedValue, type: data.type }
          rethrowDbConflict(error, ctx)
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
