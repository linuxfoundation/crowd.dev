import type { Request, Response } from 'express'
import { z } from 'zod'

import { ConflictError, NotFoundError } from '@crowd/common'
import { findMemberIdsByIdentities, optionsQx } from '@crowd/data-access-layer'
import { IMemberIdentity, MemberIdentityType, PlatformType } from '@crowd/types'

import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

const bodySchema = z.object({
  lfids: z.array(z.string().trim()).min(1, 'At least one lfid is required'),
  emails: z.array(z.email()).optional(),
})

export async function resolveMemberByIdentities(req: Request, res: Response): Promise<void> {
  const { lfids, emails } = validateOrThrow(bodySchema, req.body)

  const qx = optionsQx(req)

  const identities: Partial<IMemberIdentity>[] = [
    ...lfids.map((lfid) => ({
      platform: PlatformType.LFID,
      type: MemberIdentityType.USERNAME,
      value: lfid,
    })),
    ...(emails?.map((email) => ({ type: MemberIdentityType.EMAIL, value: email })) ?? []),
  ]

  const memberIds = await findMemberIdsByIdentities(qx, identities)

  if (memberIds.length === 0) {
    throw new NotFoundError('Member not found')
  } else if (memberIds.length > 1) {
    throw new ConflictError('Conflicting identities')
  }

  const memberId = memberIds[0]

  ok(res, { memberId })
}
