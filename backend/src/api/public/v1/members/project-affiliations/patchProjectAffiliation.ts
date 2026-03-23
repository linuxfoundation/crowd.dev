import type { Request, Response } from 'express'
import { z } from 'zod'

import { captureApiChange, memberEditAffiliationsAction } from '@crowd/audit-logs'
import { NotFoundError } from '@crowd/common'
import { CommonMemberService } from '@crowd/common_services'
import {
  MemberField,
  deleteAllMemberSegmentAffiliationsForProject,
  fetchMemberProjectSegments,
  fetchMemberSegmentAffiliationsForProject,
  findMaintainerRoles,
  findMemberById,
  insertMemberSegmentAffiliations,
  optionsQx,
} from '@crowd/data-access-layer'
import type { ISegmentAffiliationWithOrg } from '@crowd/data-access-layer'

import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { mapSegmentAffiliation } from './mappers'

const paramsSchema = z.object({
  memberId: z.uuid(),
  projectId: z.uuid(),
})

const bodySchema = z
  .object({
    affiliations: z.array(
      z
        .object({
          organizationId: z.uuid(),
          dateStart: z.coerce.date(),
          dateEnd: z.coerce.date().nullable().optional(),
        })
        .refine((a) => a.dateEnd == null || a.dateEnd >= a.dateStart, {
          message: 'dateEnd must be greater than or equal to dateStart',
        }),
    ),
    verifiedBy: z.string().max(255).optional(),
  })
  .refine((b) => b.affiliations.length === 0 || b.verifiedBy != null, {
    message: 'verifiedBy is required when affiliations is non-empty',
    path: ['verifiedBy'],
  })

export async function patchProjectAffiliation(req: Request, res: Response): Promise<void> {
  const { memberId, projectId } = validateOrThrow(paramsSchema, req.params)
  const { affiliations, verifiedBy } = validateOrThrow(bodySchema, req.body)

  const qx = optionsQx(req)

  const member = await findMemberById(qx, memberId, [MemberField.ID])
  if (!member) {
    throw new NotFoundError('Member not found')
  }

  const [segment] = await fetchMemberProjectSegments(qx, memberId, projectId)
  if (!segment) {
    throw new NotFoundError('Project not found')
  }

  const existingAffiliations = await fetchMemberSegmentAffiliationsForProject(
    qx,
    memberId,
    projectId,
  )

  let updatedAffiliations: ISegmentAffiliationWithOrg[] = []

  await captureApiChange(
    req,
    memberEditAffiliationsAction(memberId, async (captureOldState, captureNewState) => {
      captureOldState(existingAffiliations)

      await qx.tx(async (tx) => {
        await deleteAllMemberSegmentAffiliationsForProject(tx, memberId, projectId)

        if (affiliations.length > 0) {
          await insertMemberSegmentAffiliations(
            tx,
            memberId,
            projectId,
            affiliations.map((a) => ({
              organizationId: a.organizationId,
              dateStart: a.dateStart.toISOString(),
              dateEnd: a.dateEnd?.toISOString() ?? null,
              verifiedBy: verifiedBy!,
            })),
          )
        }

        const oldOrgIds = existingAffiliations.map((a) => a.organizationId)
        const newOrgIds = affiliations.map((a) => a.organizationId)
        const orgIdsToRecalculate = [...new Set([...oldOrgIds, ...newOrgIds])]

        const service = new CommonMemberService(tx, req.temporal, req.log)
        await service.startAffiliationRecalculation(memberId, orgIdsToRecalculate)
      })

      updatedAffiliations = await fetchMemberSegmentAffiliationsForProject(qx, memberId, projectId)
      captureNewState(updatedAffiliations)
    }),
  )

  const maintainerRoles = await findMaintainerRoles(qx, [memberId])

  const roles = maintainerRoles
    .filter((r) => r.segmentId === projectId)
    .map((r) => ({
      id: r.id,
      role: r.role,
      startDate: r.dateStart ?? null,
      endDate: r.dateEnd ?? null,
      repoUrl: r.url ?? null,
      repoFileUrl: r.maintainerFile ?? null,
    }))

  ok(res, {
    id: segment.id,
    projectSlug: segment.slug,
    projectName: segment.name,
    projectLogo: segment.projectLogo ?? null,
    contributionCount: Number(segment.activityCount),
    roles,
    affiliations: updatedAffiliations.map(mapSegmentAffiliation),
  })
}
