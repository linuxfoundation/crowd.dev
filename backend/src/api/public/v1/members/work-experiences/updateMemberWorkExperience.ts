import type { Request, Response } from 'express'
import { z } from 'zod'

import { captureApiChange, memberEditOrganizationsAction } from '@crowd/audit-logs'
import { NotFoundError } from '@crowd/common'
import { CommonMemberService } from '@crowd/common_services'
import {
  MemberField,
  cleanSoftDeletedMemberOrganization,
  fetchManyMemberOrgsWithOrgData,
  findMemberById,
  optionsQx,
  updateMemberOrganization,
} from '@crowd/data-access-layer'
import type { MemberOrganizationUpdate } from '@crowd/types'

import { ok } from '@/utils/api'
import { toMemberWorkExperience } from '@/utils/mapper'
import { validateOrThrow } from '@/utils/validation'

const paramsSchema = z.object({
  memberId: z.uuid(),
  workExperienceId: z.uuid(),
})

const bodySchema = z.object({
  organizationId: z.uuid(),
  jobTitle: z.string(),
  verified: z.boolean(),
  verifiedBy: z.string(),
  source: z.string(),
  startDate: z.coerce.date(),
  endDate: z.coerce.date().nullable().optional(),
})

export async function updateMemberWorkExperience(req: Request, res: Response): Promise<void> {
  const { memberId, workExperienceId } = validateOrThrow(paramsSchema, req.params)
  const data = validateOrThrow(bodySchema, req.body)

  const qx = optionsQx(req)

  const member = await findMemberById(qx, memberId, [MemberField.ID])

  if (!member) {
    throw new NotFoundError('Member not found')
  }

  const orgsMap = await fetchManyMemberOrgsWithOrgData(qx, [memberId])
  const existing = (orgsMap.get(memberId) ?? []).find((mo) => mo.id === workExperienceId)

  if (!existing) {
    throw new NotFoundError('Work experience not found')
  }

  const update: MemberOrganizationUpdate = {
    organizationId: data.organizationId,
    title: data.jobTitle,
    verified: data.verified,
    verifiedBy: data.verifiedBy,
    source: data.source,
    dateStart: data.startDate,
    dateEnd: data.endDate,
  }

  let updated: ReturnType<typeof toMemberWorkExperience> | undefined

  await captureApiChange(
    req,
    memberEditOrganizationsAction(memberId, async (captureOldState, captureNewState) => {
      captureOldState(existing)

      await qx.tx(async (tx) => {
        await cleanSoftDeletedMemberOrganization(tx, memberId, data.organizationId, update)
        await updateMemberOrganization(tx, memberId, workExperienceId, update)

        const service = new CommonMemberService(tx, req.temporal, req.log)
        await service.startAffiliationRecalculation(memberId, [data.organizationId])
      })

      const orgsMap = await fetchManyMemberOrgsWithOrgData(qx, [memberId])
      const updatedMo = (orgsMap.get(memberId) ?? []).find((mo) => mo.id === workExperienceId)

      if (!updatedMo) {
        throw new NotFoundError('Work experience not found')
      }

      captureNewState(updatedMo)
      updated = toMemberWorkExperience(updatedMo)
    }),
  )

  ok(res, updated)
}
