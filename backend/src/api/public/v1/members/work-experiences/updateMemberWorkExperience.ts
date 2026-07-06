import type { Request, Response } from 'express'
import { z } from 'zod'

import { captureApiChange, memberEditOrganizationsAction } from '@crowd/audit-logs'
import { BadRequestError, NotFoundError, sanitizeMemberOrganizationDateRange } from '@crowd/common'
import { signalMemberUpdate } from '@crowd/common_services'
import {
  MemberField,
  cleanSoftDeletedMemberOrganization,
  fetchManyMemberOrgsWithOrgData,
  fetchMemberOrganizations,
  findMemberById,
  optionsQx,
  updateMemberOrganization,
} from '@crowd/data-access-layer'
import type { MemberOrganizationDateRange, MemberOrganizationUpdate } from '@crowd/types'

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

  const memberOrgs = await fetchMemberOrganizations(qx, memberId)
  const existing = memberOrgs.find((mo) => mo.id === workExperienceId)

  if (!existing) {
    throw new NotFoundError('Work experience not found')
  }

  let dates: MemberOrganizationDateRange

  try {
    dates = sanitizeMemberOrganizationDateRange(data.startDate, data.endDate, true)
  } catch (error) {
    throw new BadRequestError('Invalid work experience date range')
  }

  const update: MemberOrganizationUpdate = {
    organizationId: data.organizationId,
    title: data.jobTitle,
    verified: data.verified,
    verifiedBy: data.verifiedBy,
    source: data.source,
    dateStart: dates.dateStart,
    dateEnd: dates.dateEnd,
  }

  let updated: ReturnType<typeof toMemberWorkExperience> | undefined

  await captureApiChange(
    req,
    memberEditOrganizationsAction(memberId, async (captureOldState, captureNewState) => {
      captureOldState(existing)

      await qx.tx(async (tx) => {
        await cleanSoftDeletedMemberOrganization(tx, memberId, data.organizationId, update)
        await updateMemberOrganization(tx, memberId, workExperienceId, update)

        const overlapBasis = { ...existing, ...update }

        const overlappingGroupedRows = getOverlappingGroupedMemberOrganizations(
          memberOrgs,
          overlapBasis,
        )

        const groupedUpdate: MemberOrganizationUpdate = {}

        // Keep grouped rows aligned for shared display fields; dates stay on the edited row
        if (data.jobTitle !== undefined) {
          groupedUpdate.title = data.jobTitle
        }
        if (data.verified !== undefined) {
          groupedUpdate.verified = data.verified
        }
        if (data.verifiedBy !== undefined) {
          groupedUpdate.verifiedBy = data.verifiedBy
        }

        if (overlappingGroupedRows.length > 0 && Object.keys(groupedUpdate).length > 0) {
          for (const overlappingRow of overlappingGroupedRows.filter(
            (row): row is typeof row & { id: string } => !!row.id,
          )) {
            await updateMemberOrganization(tx, memberId, overlappingRow.id, groupedUpdate)
          }
        }
      })

      // Signal after commit so the workflow sees persisted changes
      await signalMemberUpdate(req.temporal, memberId, {
        memberOrganizationIds: [data.organizationId],
      })

      const orgsMap = await fetchManyMemberOrgsWithOrgData(qx, [memberId])

      const updatedMo = groupMemberOrganizations(orgsMap.get(memberId) ?? []).find(
        (mo) => mo.id === workExperienceId,
      )

      if (!updatedMo) {
        throw new NotFoundError('Work experience not found')
      }

      captureNewState(updatedMo)
      updated = toMemberWorkExperience(updatedMo)
    }),
  )

  ok(res, updated)
}
