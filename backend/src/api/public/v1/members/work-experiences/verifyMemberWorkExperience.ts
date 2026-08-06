import type { Request, Response } from 'express'
import { z } from 'zod'

import { captureApiChange, memberVerifyWorkExperienceAction } from '@crowd/audit-logs'
import { NotFoundError } from '@crowd/common'
import { signalMemberUpdate } from '@crowd/common_services'
import {
  MemberField,
  deleteMemberOrganizations,
  fetchManyMemberOrgsWithOrgData,
  fetchMemberOrganizations,
  findMemberById,
  updateMemberOrganization,
} from '@crowd/data-access-layer'
import { IMemberOrganization, IMemberRoleWithOrganization } from '@crowd/types'

import { optionsQx } from '@/database/sequelizeQueryExecutor'
import { ok } from '@/utils/api'
import {
  getOverlappingGroupedMemberOrganizations,
  groupMemberOrganizations,
  toMemberWorkExperience,
} from '@/utils/mapper'
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

  const memberOrgs = await fetchMemberOrganizations(qx, memberId)
  const memberOrg = memberOrgs.find((mo) => mo.id === workExperienceId)

  if (!memberOrg) {
    throw new NotFoundError('Work experience not found')
  }

  // Stash org fields for response fallback when reject soft-deletes the row.
  const memberOrgsWithOrgDataBeforeChange = verified
    ? []
    : ((
        await fetchManyMemberOrgsWithOrgData(qx, [memberId], {
          withDomains: true,
        })
      ).get(memberId) ?? [])

  const overlappingGroupedRows = getOverlappingGroupedMemberOrganizations(memberOrgs, memberOrg)

  const overlappingRowsWithIds = overlappingGroupedRows.filter(
    (row): row is typeof row & { id: string } => !!row.id,
  )

  const memberOrgIdsToDelete = [workExperienceId, ...overlappingRowsWithIds.map((row) => row.id)]

  const verifiedUpdate = { verified, verifiedBy }

  let updatedMemberOrg: IMemberOrganization | undefined

  await captureApiChange(
    req,
    memberVerifyWorkExperienceAction(memberId, async (captureOldState, captureNewState) => {
      captureOldState(memberOrg)

      await qx.tx(async (tx) => {
        if (verified) {
          // Verification status belongs to the grouped work experience, not just the visible row
          updatedMemberOrg = await updateMemberOrganization(
            tx,
            memberId,
            workExperienceId,
            verifiedUpdate,
          )

          for (const overlappingRow of overlappingRowsWithIds) {
            await updateMemberOrganization(tx, memberId, overlappingRow.id, verifiedUpdate)
          }
        } else {
          // Unverifying removes the grouped work experience from both visible and hidden rows
          await deleteMemberOrganizations(tx, memberId, memberOrgIdsToDelete, true)
        }
      })

      // Signal after commit so the workflow sees persisted changes
      if (!verified) {
        await signalMemberUpdate(req.temporal, memberId, {
          memberOrganizationIds: [memberOrg.organizationId],
        })
      }

      captureNewState(updatedMemberOrg ?? { ...memberOrg, ...verifiedUpdate })
    }),
  )

  const orgsMap = await fetchManyMemberOrgsWithOrgData(qx, [memberId], {
    withDomains: true,
  })

  const groupedMemberOrgs = groupMemberOrganizations(orgsMap.get(memberId) ?? [])
  const groupedMemberOrgsBeforeChange = groupMemberOrganizations(memberOrgsWithOrgDataBeforeChange)

  const fallbackMo = groupedMemberOrgsBeforeChange.find((mo) => mo.id === workExperienceId)

  const responseMo: IMemberRoleWithOrganization =
    groupedMemberOrgs.find((mo) => mo.id === workExperienceId) ??
    (fallbackMo ? { ...fallbackMo, ...verifiedUpdate } : undefined)

  if (!responseMo) {
    throw new NotFoundError('Work experience not found')
  }

  ok(res, toMemberWorkExperience(responseMo))
}
