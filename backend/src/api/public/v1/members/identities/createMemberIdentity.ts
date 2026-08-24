import type { Request, Response } from 'express'
import { z } from 'zod'

import { captureApiChange, memberEditIdentitiesAction } from '@crowd/audit-logs'
import { ConflictError, NotFoundError, normalizeMemberIdentityValue } from '@crowd/common'
import {
  MemberField,
  findMemberById,
  findMemberIdByVerifiedIdentity,
  findMemberIdentitiesByValue,
  findMemberIdentityConflict,
  insertMemberIdentities,
  touchMemberUpdatedAt,
  updateMemberIdentity,
} from '@crowd/data-access-layer'
import { IMemberIdentity, MemberIdentityType } from '@crowd/types'

import { optionsQx } from '@/database/sequelizeQueryExecutor'
import { created, ok } from '@/utils/api'
import { isMemberIdentityDbConflict, rethrowDbConflict } from '@/utils/err'
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
    verifiedBy: z.string().trim().min(1).optional(),
  })
  .refine((data) => !data.verified || data.verifiedBy, {
    message: 'verifiedBy is required when verified is true',
    path: ['verifiedBy'],
  })

export async function createMemberIdentity(req: Request, res: Response): Promise<void> {
  const { memberId } = validateOrThrow(paramsSchema, req.params)
  const raw = validateOrThrow(bodySchema, req.body)
  const data = {
    ...raw,
    value: normalizeMemberIdentityValue(raw.value),
  }

  const qx = optionsQx(req)
  const member = await findMemberById(qx, memberId, [MemberField.ID])
  if (!member) {
    throw new NotFoundError('Member not found')
  }

  const conflictContext = {
    memberId,
    platform: data.platform,
    value: data.value,
    type: data.type,
  }

  let identity!: IMemberIdentity
  let alreadyExisted = false

  await captureApiChange(
    req,
    memberEditIdentitiesAction(memberId, async (captureOldState, captureNewState) => {
      captureOldState({})

      let outcome: { identity: IMemberIdentity; alreadyExisted: boolean }

      try {
        outcome = await qx.tx(async (tx) => {
          const existing = await findMemberIdentitiesByValue(tx, memberId, data.value, {
            type: data.type,
          })

          const exactMatch = existing.find((row) => row.platform === data.platform)

          let result = exactMatch
          const existed = Boolean(exactMatch)

          // Unverified identities aren't unique in the db, so the same handle or
          // email can sit on several members. Reject it here if someone else has it.
          if (!result && !data.verified) {
            const conflict = await findMemberIdentityConflict(tx, {
              value: data.value,
              platform: data.platform,
              type: data.type,
              excludeMemberId: memberId,
            })

            if (conflict) {
              throw new ConflictError('Identity already exists on another member', {
                ...conflictContext,
                conflictMemberId: conflict.memberId,
              })
            }
          }

          if (!result) {
            const [inserted] = await insertMemberIdentities(
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

            result = inserted
          }

          // A verified identity confirms the same value for this member, so keep same-value
          // identities in sync instead of leaving stale unverified duplicates behind.
          if (data.verified && existing.length > 0) {
            const updatedRows = await Promise.all(
              existing.map((row) =>
                updateMemberIdentity(tx, memberId, row.id, {
                  verified: true,
                  verifiedBy: data.verifiedBy,
                }),
              ),
            )

            const updatedExact = updatedRows.find((row) => row?.id === exactMatch?.id)

            if (updatedExact) {
              result = updatedExact
            }
          }

          await touchMemberUpdatedAt(tx, memberId)

          return { identity: result, alreadyExisted: existed }
        })
      } catch (error) {
        if (!isMemberIdentityDbConflict(error)) {
          throw error
        }

        const existing = await findMemberIdentitiesByValue(qx, memberId, data.value, {
          type: data.type,
        })
        const exactMatch = existing.find((row) => row.platform === data.platform)

        if (exactMatch) {
          outcome = { identity: exactMatch, alreadyExisted: true }
        } else {
          const conflictMemberId = await findMemberIdByVerifiedIdentity(
            qx,
            data.platform,
            data.value,
            data.type,
          )

          rethrowDbConflict(error, {
            ...conflictContext,
            ...(conflictMemberId ? { conflictMemberId } : {}),
          })
        }
      }

      identity = outcome.identity
      alreadyExisted = outcome.alreadyExisted

      captureNewState(identity)
    }),
  )

  const response = {
    id: identity.id,
    value: identity.value,
    platform: identity.platform,
    type: identity.type,
    verified: identity.verified,
    verifiedBy: identity.verifiedBy ?? null,
    source: identity.source ?? null,
    createdAt: identity.createdAt,
    updatedAt: identity.updatedAt,
  }

  if (alreadyExisted) {
    ok(res, response)
  } else {
    created(res, response)
  }
}
