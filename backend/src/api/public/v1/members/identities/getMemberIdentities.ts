import type { Request, Response } from 'express'
import { z } from 'zod'

import { NotFoundError } from '@crowd/common'
import {
  MemberField,
  fetchMemberIdentities,
  findMemberById,
  optionsQx,
} from '@crowd/data-access-layer'

import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

const paramsSchema = z.object({
  memberId: z.uuid(),
})

export async function getMemberIdentities(req: Request, res: Response): Promise<void> {
  const { memberId } = validateOrThrow(paramsSchema, req.params)
  const qx = optionsQx(req)

  const member = await findMemberById(qx, memberId, [MemberField.ID])

  if (!member) throw new NotFoundError('Member not found')

  const rawIdentities = await fetchMemberIdentities(qx, memberId)

  const identities = rawIdentities.map(
    ({ id, value, platform, verified, verifiedBy, source, createdAt, updatedAt }) => ({
      id,
      value,
      platform,
      verified,
      verifiedBy: verifiedBy ?? null,
      source,
      createdAt,
      updatedAt,
    }),
  )

  ok(res, { identities })
}
