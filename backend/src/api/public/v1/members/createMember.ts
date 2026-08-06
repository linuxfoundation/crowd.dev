import type { Request, Response } from 'express'
import { z } from 'zod'

import { captureApiChange, memberCreateAction, memberEditIdentitiesAction } from '@crowd/audit-logs'
import { getProperDisplayName } from '@crowd/common'
import {
  findMemberIdByVerifiedIdentity,
  createMember as insertMember,
  insertMemberIdentities,
} from '@crowd/data-access-layer'
import { MemberIdentityType } from '@crowd/types'

import { optionsQx } from '@/database/sequelizeQueryExecutor'
import { created } from '@/utils/api'
import { isMemberIdentityDbConflict, rethrowDbConflict } from '@/utils/err'
import { validateOrThrow } from '@/utils/validation'

const bodySchema = z.object({
  displayName: z.string().trim().min(1),
  identities: z
    .array(
      z
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
        }),
    )
    .min(1),
})

export async function createMember(req: Request, res: Response): Promise<void> {
  const { displayName, identities } = validateOrThrow(bodySchema, req.body)
  const qx = optionsQx(req)

  const normalizedDisplayName = getProperDisplayName(displayName)

  const { dbMember, dbIdentities } = await qx.tx(async (tx) => {
    try {
      const dbMember = await insertMember(tx, {
        displayName: normalizedDisplayName,
        joinedAt: new Date().toISOString(),
        attributes: {},
        reach: {},
        // OpenSearch sync only keeps members that either have activities or have manuallyCreated set.
        manuallyCreated: true,
      })

      const dbIdentities = await insertMemberIdentities(
        tx,
        identities.map((identity) => ({
          ...identity,
          memberId: dbMember.id,
        })),
        true,
        true,
      )

      return { dbMember, dbIdentities }
    } catch (error) {
      // Only notify for a single identity because we can't tell which one conflicted in a batch.
      if (identities.length === 1 && isMemberIdentityDbConflict(error)) {
        const identity = identities[0]
        const conflictMemberId = await findMemberIdByVerifiedIdentity(
          qx,
          identity.platform,
          identity.value,
          identity.type,
        )

        return rethrowDbConflict(error, {
          ...(conflictMemberId ? { conflictMemberId } : {}),
          platform: identity.platform,
          value: identity.value,
          type: identity.type,
        })
      }

      return rethrowDbConflict(error)
    }
  })

  await captureApiChange(
    req,
    memberCreateAction(dbMember.id, async (captureNewState) => {
      captureNewState({
        memberId: dbMember.id,
        displayName: dbMember.displayName,
        manuallyCreated: true,
      })
    }),
  )

  await captureApiChange(
    req,
    memberEditIdentitiesAction(dbMember.id, async (captureOldState, captureNewState) => {
      captureOldState({})
      captureNewState(dbIdentities)
    }),
  )

  created(res, { memberId: dbMember.id })
}
