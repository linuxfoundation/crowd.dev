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
  optionsQx,
  updateMemberOrganization,
} from '@crowd/data-access-layer'
import { IMemberOrganization, IMemberRoleWithOrganization } from '@crowd/types'

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

  const overlappingGroupedRows = getOverlappingGroupedMemberOrganizations(memberOrgs, memberOrg)

  const memberOrgIdsToDelete = [
    workExperienceId,
    ...overlappingGroupedRows.flatMap((row) => (row.id ? [row.id] : [])),
  ]

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

          for (const overlappingRow of overlappingGroupedRows.filter(
            (row): row is typeof row & { id: string } => !!row.id,
          )) {
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

      captureNewState(updatedMemberOrg ?? { ...memberOrg, verified, verifiedBy })
    }),
  )

  const orgsMap = await fetchManyMemberOrgsWithOrgData(qx, [memberId], { withDomains: true })
  const memberOrgsWithData = orgsMap.get(memberId) ?? []

  const responseMo: IMemberRoleWithOrganization =
    groupMemberOrganizations(memberOrgsWithData).find((mo) => mo.id === workExperienceId) ??
    ({
      ...(memberOrgsWithData.find((mo) => mo.id === workExperienceId) ?? memberOrg),
      ...updatedMemberOrg,
      verified,
      verifiedBy,
    } as IMemberRoleWithOrganization)

  ok(res, toMemberWorkExperience(responseMo))
}
