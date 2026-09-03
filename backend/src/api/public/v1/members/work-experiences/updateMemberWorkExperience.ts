import type { Request, Response } from 'express'
import { z } from 'zod'

import { captureApiChange, memberEditOrganizationsAction } from '@crowd/audit-logs'
import {
  BadRequestError,
  ConflictError,
  NotFoundError,
  sanitizeMemberOrganizationDateRange,
} from '@crowd/common'
import { normalizeMemberOrganizationDate, signalMemberUpdate } from '@crowd/common_services'
import {
  MemberField,
  cleanSoftDeletedMemberOrganization,
  deleteMemberOrganizations,
  fetchManyMemberOrgsWithOrgData,
  fetchMemberOrganizations,
  findMemberById,
  updateMemberOrganization,
} from '@crowd/data-access-layer'
import type {
  IMemberOrganization,
  MemberOrganizationDateRange,
  MemberOrganizationUpdate,
} from '@crowd/types'

import { optionsQx } from '@/database/sequelizeQueryExecutor'
import { ok } from '@/utils/api'
import {
  getOverlappingGroupedMemberOrganizations,
  groupMemberOrganizations,
  isCollapsibleMemberOrganization,
  toMemberWorkExperience,
} from '@/utils/mapper'
import { validateOrThrow } from '@/utils/validation'

/** Matches the active unique index on memberOrganizations (org + date range). */
function sameUniqueKey(
  a: Pick<IMemberOrganization, 'organizationId' | 'dateStart' | 'dateEnd'>,
  b: Pick<IMemberOrganization, 'organizationId' | 'dateStart' | 'dateEnd'>,
): boolean {
  return (
    a.organizationId === b.organizationId &&
    normalizeMemberOrganizationDate(a.dateStart) === normalizeMemberOrganizationDate(b.dateStart) &&
    normalizeMemberOrganizationDate(a.dateEnd) === normalizeMemberOrganizationDate(b.dateEnd)
  )
}

const paramsSchema = z.object({
  memberId: z.uuid(),
  workExperienceId: z.uuid(),
})

const bodySchema = z.object({
  organizationId: z.uuid(),
  jobTitle: z.string(),
  verified: z.boolean(),
  verifiedBy: z.string().trim().min(1),
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
        // Avoid unique-index collisions before we UPDATE the visible row.
        const conflictingRows = memberOrgs.filter(
          (row) =>
            !!row.id &&
            row.id !== workExperienceId &&
            sameUniqueKey(row, {
              organizationId: data.organizationId,
              dateStart: dates.dateStart,
              dateEnd: dates.dateEnd,
            }),
        )

        // Conflict if a visible work experience with the same dates already exists. Throw a conflict error.
        const conflictingVisibleIds = conflictingRows
          .filter((row) => !isCollapsibleMemberOrganization(row))
          .map((row) => row.id)
          .filter((id): id is string => !!id)

        if (conflictingVisibleIds.length > 0) {
          throw new ConflictError('A work experience with the same dates already exists')
        }

        // Hidden leftovers were merged into this card. Remove anything that overlapped
        // the old dates or the new ones, so shrinking a range doesn't leave a ghost job.
        const overlappingIds = [
          ...new Set(
            [
              ...getOverlappingGroupedMemberOrganizations(memberOrgs, existing),
              ...getOverlappingGroupedMemberOrganizations(memberOrgs, {
                ...existing,
                ...update,
              }),
            ].flatMap((row) => (row.id ? [row.id] : [])),
          ),
        ]

        if (overlappingIds.length > 0) {
          await deleteMemberOrganizations(tx, memberId, overlappingIds, true, data.verifiedBy)
        }

        await cleanSoftDeletedMemberOrganization(tx, memberId, data.organizationId, update)
        await updateMemberOrganization(tx, memberId, workExperienceId, update)
      })

      // Signal after commit so the workflow sees persisted changes
      await signalMemberUpdate(req.temporal, memberId, {
        memberOrganizationIds: [data.organizationId],
      })

      const orgsMap = await fetchManyMemberOrgsWithOrgData(qx, [memberId], { withDomains: true })

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
