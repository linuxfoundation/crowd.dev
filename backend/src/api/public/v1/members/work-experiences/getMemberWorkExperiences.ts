import type { Request, Response } from 'express'
import { z } from 'zod'

import { NotFoundError } from '@crowd/common'
import {
  MemberField,
  fetchManyMemberOrgsWithOrgData,
  findMemberById,
  optionsQx,
} from '@crowd/data-access-layer'

import { ok } from '@/utils/api'
import { toMemberWorkExperience } from '@/utils/mapper'
import { validateOrThrow } from '@/utils/validation'

const paramsSchema = z.object({
  memberId: z.uuid(),
})

export async function getMemberWorkExperiences(req: Request, res: Response): Promise<void> {
  const { memberId } = validateOrThrow(paramsSchema, req.params)
  const qx = optionsQx(req)

  const member = await findMemberById(qx, memberId, [MemberField.ID])

  if (!member) {
    throw new NotFoundError('Member not found')
  }

  const orgsMap = await fetchManyMemberOrgsWithOrgData(qx, [memberId])
  const workExperiences = (orgsMap.get(memberId) ?? []).map(toMemberWorkExperience)

  ok(res, { memberId, workExperiences })
}
