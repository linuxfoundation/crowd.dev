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
} from '@crowd/data-access-layer'

import { optionsQx } from '@/database/sequelizeQueryExecutor'
import { noContent } from '@/utils/api'
import { getOverlappingGroupedMemberOrganizations } from '@/utils/mapper'
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

  const overlappingGroupedRows = getOverlappingGroupedMemberOrganizations(memberOrgs, memberOrg)

  const memberOrgIdsToDelete = [
    workExperienceId,
    ...overlappingGroupedRows.flatMap((row) => (row.id ? [row.id] : [])),
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
