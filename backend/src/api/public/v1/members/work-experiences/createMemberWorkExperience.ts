import type { Request, Response } from 'express'
import { z } from 'zod'

import { captureApiChange, memberEditOrganizationsAction } from '@crowd/audit-logs'
import { ConflictError, NotFoundError } from '@crowd/common'
import { CommonMemberService } from '@crowd/common_services'
import {
  MemberField,
  changeMemberOrganizationAffiliationOverrides,
  checkOrganizationAffiliationPolicy,
  cleanSoftDeletedMemberOrganization,
  createMemberOrganization,
  fetchManyMemberOrgsWithOrgData,
  findMemberById,
  optionsQx,
} from '@crowd/data-access-layer'
import type { IMemberOrganization, IMemberRoleWithOrganization } from '@crowd/types'

import { created } from '@/utils/api'
import { toMemberWorkExperience } from '@/utils/mapper'
import { validateOrThrow } from '@/utils/validation'

const paramsSchema = z.object({
  memberId: z.uuid(),
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

export async function createMemberWorkExperience(req: Request, res: Response): Promise<void> {
  const { memberId } = validateOrThrow(paramsSchema, req.params)
  const data = validateOrThrow(bodySchema, req.body)

  const qx = optionsQx(req)

  const member = await findMemberById(qx, memberId, [MemberField.ID])

  if (!member) {
    throw new NotFoundError('Member not found')
  }

  let createdMo: IMemberRoleWithOrganization | undefined

  await captureApiChange(
    req,
    memberEditOrganizationsAction(memberId, async (captureOldState, captureNewState) => {
      captureOldState({})

      const memberOrgData: IMemberOrganization = {
        memberId,
        organizationId: data.organizationId,
        title: data.jobTitle,
        dateStart: data.startDate,
        dateEnd: data.endDate,
        source: data.source,
        verified: data.verified,
        verifiedBy: data.verifiedBy,
      }

      let newMemberOrgId: string | undefined

      await qx.tx(async (tx) => {
        await cleanSoftDeletedMemberOrganization(tx, memberId, data.organizationId, data)

        newMemberOrgId = await createMemberOrganization(tx, memberId, memberOrgData)

        if (!newMemberOrgId) {
          throw new ConflictError('A work experience with the same dates already exists')
        }

        const isAffiliationBlocked = await checkOrganizationAffiliationPolicy(
          tx,
          data.organizationId,
        )

        if (newMemberOrgId && isAffiliationBlocked) {
          await changeMemberOrganizationAffiliationOverrides(tx, [
            {
              memberId,
              memberOrganizationId: newMemberOrgId,
              allowAffiliation: false,
            },
          ])
        }

        const service = new CommonMemberService(tx, req.temporal, req.log)
        await service.startAffiliationRecalculation(memberId, [data.organizationId])
      })

      const orgsMap = await fetchManyMemberOrgsWithOrgData(qx, [memberId])
      createdMo = (orgsMap.get(memberId) ?? []).find((mo) => mo.id === newMemberOrgId)

      captureNewState(createdMo ?? null)
    }),
  )

  if (!createdMo) {
    throw new NotFoundError('Work experience not found after creation')
  }

  created(res, toMemberWorkExperience(createdMo))
}
