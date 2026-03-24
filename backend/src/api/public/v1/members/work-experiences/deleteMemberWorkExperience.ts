import type { Request, Response } from 'express'
import { z } from 'zod'

import { captureApiChange, memberEditOrganizationsAction } from '@crowd/audit-logs'
import { NotFoundError } from '@crowd/common'
import { CommonMemberService } from '@crowd/common_services'
import {
  MemberField,
  deleteMemberOrganizations,
  fetchManyMemberOrgsWithOrgData,
  findMemberById,
  optionsQx,
} from '@crowd/data-access-layer'

import { noContent } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

const paramsSchema = z.object({
  memberId: z.uuid(),
  workExperienceId: z.uuid(),
})

export async function deleteMemberWorkExperience(req: Request, res: Response): Promise<void> {
  const { memberId, workExperienceId } = validateOrThrow(paramsSchema, req.params)

  const qx = optionsQx(req)

  const member = await findMemberById(qx, memberId, [MemberField.ID])

  if (!member) {
    throw new NotFoundError('Member not found')
  }

  const orgsMap = await fetchManyMemberOrgsWithOrgData(qx, [memberId])

  const memberOrg = (orgsMap.get(memberId) ?? []).find((mo) => mo.id === workExperienceId)

  if (!memberOrg) {
    throw new NotFoundError('Work experience not found')
  }

  await captureApiChange(
    req,
    memberEditOrganizationsAction(memberId, async (captureOldState, captureNewState) => {
      captureOldState(memberOrg)

      await qx.tx(async (tx) => {
        await deleteMemberOrganizations(tx, memberId, [workExperienceId])
        const commonMemberService = new CommonMemberService(tx, req.temporal, req.log)
        await commonMemberService.startAffiliationRecalculation(memberId, [
          memberOrg.organizationId,
        ])
      })

      captureNewState(null)
    }),
  )
  noContent(res)
}
