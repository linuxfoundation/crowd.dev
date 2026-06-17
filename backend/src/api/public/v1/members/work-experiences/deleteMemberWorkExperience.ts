import type { Request, Response } from 'express'
import { z } from 'zod'

import { captureApiChange, memberEditOrganizationsAction } from '@crowd/audit-logs'
import { NotFoundError } from '@crowd/common'
import { signalMemberUpdate } from '@crowd/common_services'
import {
  MemberField,
  deleteMemberOrganizations,
  fetchMemberOrganizations,
  findMemberById,
  optionsQx,
} from '@crowd/data-access-layer'

import { noContent } from '@/utils/api'
import { getOverlappingEmailDomainMemberOrganizations } from '@/utils/mapper'
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

  const memberOrgs = await fetchMemberOrganizations(qx, memberId)
  const memberOrg = memberOrgs.find((mo) => mo.id === workExperienceId)

  if (!memberOrg) {
    throw new NotFoundError('Work experience not found')
  }

  const overlappingEmailDomainRows = getOverlappingEmailDomainMemberOrganizations(
    memberOrgs,
    memberOrg,
  )

  const memberOrgIdsToDelete = [
    workExperienceId,
    ...overlappingEmailDomainRows.map((row) => row.id as string),
  ]

  // Delete hidden grouped rows with the visible row so read responses stay consistent
  await captureApiChange(
    req,
    memberEditOrganizationsAction(memberId, async (captureOldState, captureNewState) => {
      captureOldState(memberOrg)

      await qx.tx(async (tx) => {
        await deleteMemberOrganizations(tx, memberId, memberOrgIdsToDelete)
      })

      // Signal after commit so the workflow sees persisted changes
      await signalMemberUpdate(req.temporal, memberId, {
        memberOrganizationIds: [memberOrg.organizationId],
      })

      captureNewState(null)
    }),
  )
  noContent(res)
}
