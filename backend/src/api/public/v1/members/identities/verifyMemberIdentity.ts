import type { Request, Response } from 'express'
import { z } from 'zod'

import {
  captureApiChange,
  memberUnmergeAction,
  memberVerifyIdentityAction,
} from '@crowd/audit-logs'
import { InternalError, NotFoundError } from '@crowd/common'
import {
  invalidateMemberQueryCache,
  prepareMemberUnmerge,
  startMemberUnmergeWorkflow,
  unmergeMember,
} from '@crowd/common_services'
import {
  MemberField,
  deleteMemberIdentity,
  findMemberById,
  findMemberIdentityById,
  optionsQx,
  queryActivityRelations,
  updateMemberIdentity,
} from '@crowd/data-access-layer'
import { SlackChannel, SlackPersona, sendSlackNotification } from '@crowd/slack'
import {
  IMemberIdentity,
  IMemberUnmergePreviewResult,
  IUnmergePreviewResult,
  MemberUnmergeResult,
} from '@crowd/types'

import { noContent, ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

const paramsSchema = z.object({
  memberId: z.uuid(),
  identityId: z.uuid(),
})

const bodySchema = z.object({
  verified: z.boolean(),
  verifiedBy: z.string(),
})

type MemberUnmergeContext = {
  preview: IUnmergePreviewResult<IMemberUnmergePreviewResult>
  result: MemberUnmergeResult
}

function toReturn(identity: IMemberIdentity) {
  return {
    id: identity.id,
    value: identity.value,
    platform: identity.platform,
    verified: identity.verified,
    verifiedBy: identity.verifiedBy ?? null,
    source: identity.source,
    createdAt: identity.createdAt,
    updatedAt: identity.updatedAt,
  }
}

export async function verifyMemberIdentity(req: Request, res: Response): Promise<void> {
  const { memberId, identityId } = validateOrThrow(paramsSchema, req.params)
  const { verified, verifiedBy } = validateOrThrow(bodySchema, req.body)
  const qx = optionsQx(req)

  const member = await findMemberById(qx, memberId, [MemberField.ID])

  if (!member) {
    throw new NotFoundError('Member not found')
  }

  const identity = await findMemberIdentityById(qx, memberId, identityId)

  if (!identity) throw new NotFoundError('Member identity not found')

  let unmerge: MemberUnmergeContext | undefined
  let updatedIdentity: IMemberIdentity | undefined

  await captureApiChange(
    req,
    memberVerifyIdentityAction(memberId, async (captureOldState, captureNewState) => {
      captureOldState(identity)

      await qx.tx(async (tx) => {
        updatedIdentity = await updateMemberIdentity(tx, memberId, identityId, {
          verified,
          verifiedBy,
        })

        if (!updatedIdentity) {
          throw new InternalError('Failed to update member identity')
        }

        if (!verified) {
          const { count } = await queryActivityRelations(tx, {
            filter: {
              and: [
                {
                  username: { eq: identity.value },
                  platform: { eq: identity.platform },
                },
              ],
            },
            limit: 1,
            countOnly: true,
          })

          if (count === 0) {
            await deleteMemberIdentity(tx, memberId, identityId)
          } else {
            const preview = await prepareMemberUnmerge(tx, memberId, identityId, false)
            const result = await unmergeMember(tx, memberId, preview, req.actor.id)
            unmerge = { preview, result }
          }
        }
      })

      captureNewState(updatedIdentity)
    }),
  )

  if (unmerge) {
    const { preview, result } = unmerge

    try {
      await captureApiChange(
        req,
        memberUnmergeAction(memberId, async (captureOldState, captureNewState) => {
          captureOldState({ primary: preview.primary })
          captureNewState({
            primary: result.primary,
            secondary: result.secondary,
          })
        }),
      )
    } catch (error) {
      req.log.warn({ error }, 'Audit log capture failed after identity unmerge')
      sendSlackNotification(
        SlackChannel.CDP_ALERTS,
        SlackPersona.ERROR_REPORTER,
        `Audit log capture failed after identity unmerge: member ${memberId}`,
        [{ title: 'Error', text: `\`${error?.message || error}\`` }],
      )
    }

    try {
      await invalidateMemberQueryCache(req.redis, [result.primary.id, result.secondary.id], true)
    } catch (error) {
      req.log.warn({ error }, 'Cache invalidation failed after identity unmerge')
    }

    try {
      await startMemberUnmergeWorkflow(req.temporal, {
        primaryId: result.primary.id,
        secondaryId: result.secondary.id,
        movedIdentities: result.movedIdentities,
        primaryDisplayName: result.primary.displayName,
        secondaryDisplayName: result.secondary.displayName,
        actorId: req.actor.id,
      })
    } catch (error) {
      req.log.warn({ error }, 'Failed to start unmerge workflow after identity unmerge')
      sendSlackNotification(
        SlackChannel.CDP_ALERTS,
        SlackPersona.ERROR_REPORTER,
        `Failed to start unmerge workflow after identity unmerge: member ${memberId}`,
        [
          {
            title: 'Context',
            text: `*Primary:* \`${result.primary.id}\`\n*Secondary:* \`${result.secondary.id}\``,
          },
          { title: 'Error', text: `\`${error?.message || error}\`` },
        ],
      )
    }
  }

  // If verified = false and no activities (deleted): 204 No Content
  if (!verified && !unmerge) {
    noContent(res)
    return
  }

  // If verified = false and has activities (unmerge): 200 OK + unmergedToMemberId
  ok(res, {
    ...toReturn(updatedIdentity),
    ...(unmerge && { unmergedToMemberId: unmerge.result.secondary.id }),
  })
}
