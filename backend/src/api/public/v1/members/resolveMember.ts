import type { Request, Response } from 'express'
import { z } from 'zod'

import { ConflictError, NotFoundError } from '@crowd/common'
import { fetchMemberIdentities, findMemberIdsByIdentities } from '@crowd/data-access-layer'
import { IMemberIdentity, MemberIdentityType, PlatformType } from '@crowd/types'

import { optionsQx } from '@/database/sequelizeQueryExecutor'
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
      verified: true,
    })),
    ...(emails?.map((email) => ({
      type: MemberIdentityType.EMAIL,
      value: email,
      verified: true,
    })) ?? []),
  ]

  const memberIds = await findMemberIdsByIdentities(qx, identities)

  if (memberIds.length === 0) {
    throw new NotFoundError('Member not found')
  } else if (memberIds.length > 1) {
    throw new ConflictError('Multiple member profiles matched', {
      reason: 'multi-match',
      memberIds,
    })
  }

  const memberId = memberIds[0]

  if (emails?.length) {
    const memberIdentities = await fetchMemberIdentities(qx, memberId)
    const memberLfids = memberIdentities
      .filter(
        (identity) =>
          identity.verified &&
          identity.platform === PlatformType.LFID &&
          identity.type === MemberIdentityType.USERNAME,
      )
      .map((identity) => identity.value)

    const suppliedLfids = new Set(lfids.map((lfid) => lfid.toLowerCase()))
    const holdsSuppliedLfid = memberLfids.some((lfid) => suppliedLfids.has(lfid.toLowerCase()))

    // Email can match a member that was never looked up by LFID. If that member
    // already has a different verified LFID, treat it as a conflict.
    if (memberLfids.length > 0 && !holdsSuppliedLfid) {
      throw new ConflictError('Member holds a different LFID', {
        reason: 'foreign-lfid',
        lfids: memberLfids,
      })
    }
  }

  ok(res, { memberId })
}
