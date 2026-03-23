import type { Request, Response } from 'express'
import { z } from 'zod'

import { captureApiChange, memberVerifyWorkExperienceAction } from '@crowd/audit-logs'
import { NotFoundError } from '@crowd/common'
import { CommonMemberService } from '@crowd/common_services'
import {
  MemberField,
  deleteMemberOrganizations,
  fetchManyMemberOrgsWithOrgData,
  findMemberById,
  optionsQx,
  updateMemberOrganization,
} from '@crowd/data-access-layer'
import { IMemberOrganization } from '@crowd/types'

import { ok } from '@/utils/api'
import { toMemberWorkExperience } from '@/utils/mapper'
import { validateOrThrow } from '@/utils/validation'

const paramsSchema = z.object({
  memberId: z.uuid(),
  workExperienceId: z.uuid(),
})

const bodySchema = z.object({
  verified: z.boolean(),
  verifiedBy: z.string(),
})

export async function verifyMemberWorkExperience(req: Request, res: Response): Promise<void> {
  const { memberId, workExperienceId } = validateOrThrow(paramsSchema, req.params)
  const { verified, verifiedBy } = validateOrThrow(bodySchema, req.body)

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

  let updatedMemberOrg: IMemberOrganization | undefined

  await captureApiChange(
    req,
    memberVerifyWorkExperienceAction(memberId, async (captureOldState, captureNewState) => {
      captureOldState(memberOrg)

      await qx.tx(async (tx) => {
        if (verified) {
          updatedMemberOrg = await updateMemberOrganization(tx, memberId, workExperienceId, {
            verified,
            verifiedBy,
          })
        } else {
          await deleteMemberOrganizations(tx, memberId, [workExperienceId], true)

          const service = new CommonMemberService(tx, req.temporal, req.log)
          await service.startAffiliationRecalculation(memberId, [memberOrg.organizationId])
        }
      })

      captureNewState(updatedMemberOrg ?? { ...memberOrg, verified, verifiedBy })
    }),
  )

  ok(res, toMemberWorkExperience({ ...memberOrg, ...updatedMemberOrg }))
}
