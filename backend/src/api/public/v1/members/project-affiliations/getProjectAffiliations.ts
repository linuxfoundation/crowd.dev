import type { Request, Response } from 'express'
import { z } from 'zod'

import { NotFoundError } from '@crowd/common'
import {
  MemberField,
  fetchMemberProjectSegments,
  fetchMemberSegmentAffiliationsWithOrg,
  fetchMemberWorkExperienceAffiliations,
  findMaintainerRoles,
  findMemberById,
  optionsQx,
} from '@crowd/data-access-layer'

import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { mapSegmentAffiliation, mapWorkExperienceAffiliation } from './mappers'

const paramsSchema = z.object({
  memberId: z.uuid(),
})

export async function getProjectAffiliations(req: Request, res: Response): Promise<void> {
  const { memberId } = validateOrThrow(paramsSchema, req.params)
  const qx = optionsQx(req)

  const member = await findMemberById(qx, memberId, [MemberField.ID])

  if (!member) {
    throw new NotFoundError('Member not found')
  }

  const [projectSegments, maintainerRoles, segmentAffiliations, workExperiences] =
    await Promise.all([
      fetchMemberProjectSegments(qx, memberId),
      findMaintainerRoles(qx, [memberId]),
      fetchMemberSegmentAffiliationsWithOrg(qx, memberId),
      fetchMemberWorkExperienceAffiliations(qx, memberId),
    ])

  // Group maintainer roles by segmentId
  const rolesBySegment = new Map<string, typeof maintainerRoles>()
  for (const role of maintainerRoles) {
    const existing = rolesBySegment.get(role.segmentId) ?? []
    existing.push(role)
    rolesBySegment.set(role.segmentId, existing)
  }

  // Group segment affiliations by segmentId
  const affiliationsBySegment = new Map<string, typeof segmentAffiliations>()
  for (const aff of segmentAffiliations) {
    const existing = affiliationsBySegment.get(aff.segmentId) ?? []
    existing.push(aff)
    affiliationsBySegment.set(aff.segmentId, existing)
  }

  const projectAffiliations = projectSegments.map((segment) => {
    const roles = (rolesBySegment.get(segment.id) ?? []).map((r) => ({
      id: r.id,
      role: r.role,
      startDate: r.dateStart ?? null,
      endDate: r.dateEnd ?? null,
      repoUrl: r.url ?? null,
      repoFileUrl: r.maintainerFile ?? null,
    }))

    // Use segment affiliations if they exist for this project, otherwise fall back to work experiences
    const segmentAffs = affiliationsBySegment.get(segment.id)
    const affiliations = segmentAffs
      ? segmentAffs.map(mapSegmentAffiliation)
      : workExperiences.map(mapWorkExperienceAffiliation)

    return {
      id: segment.id,
      projectSlug: segment.slug,
      projectName: segment.name,
      projectLogo: segment.projectLogo ?? null,
      contributionCount: Number(segment.activityCount),
      roles,
      affiliations,
    }
  })

  ok(res, { projectAffiliations })
}
