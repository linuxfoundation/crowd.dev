import type { Request, Response } from 'express'
import { z } from 'zod'

import { NotFoundError } from '@crowd/common'
import {
  MemberField,
  findMaintainerRoles,
  findMemberById,
  optionsQx,
} from '@crowd/data-access-layer'

import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

const paramsSchema = z.object({
  memberId: z.uuid(),
})

export async function getMemberMaintainerRoles(req: Request, res: Response): Promise<void> {
  const { memberId } = validateOrThrow(paramsSchema, req.params)
  const qx = optionsQx(req)

  const member = await findMemberById(qx, memberId, [MemberField.ID])

  if (!member) {
    throw new NotFoundError('Member not found')
  }

  const maintainerRoles = await findMaintainerRoles(qx, [memberId])

  ok(res, { maintainerRoles })
}
