import type { Request, Response } from 'express'
import { z } from 'zod'

import { NotFoundError } from '@crowd/common'
import {
  MemberField,
  fetchManyMemberOrgsWithOrgData,
  fetchManyOrganizationVerifiedPrimaryDomains,
  findMemberById,
  optionsQx,
} from '@crowd/data-access-layer'

import { ok } from '@/utils/api'
import { groupMemberOrganizations, toMemberWorkExperience } from '@/utils/mapper'
import { validateOrThrow } from '@/utils/validation'

const paramsSchema = z.object({
  memberId: z.uuid(),
})

export async function getMemberWorkExperiences(req: Request, res: Response): Promise<void> {
  const { memberId } = validateOrThrow(paramsSchema, req.params)
  const qx = optionsQx(req)

  const member = await findMemberById(qx, memberId, [MemberField.ID])

  if (!member) {
    throw new NotFoundError('Member not found')
  }

  const orgsMap = await fetchManyMemberOrgsWithOrgData(qx, [memberId])
  const grouped = groupMemberOrganizations(orgsMap.get(memberId) ?? [])

  const orgIds = [...new Set(grouped.map((r) => r.organizationId).filter(Boolean))]
  const primaryDomains = await fetchManyOrganizationVerifiedPrimaryDomains(qx, orgIds)
  const domainsMap = new Map(primaryDomains.map((d) => [d.orgId, d.domains]))

  const workExperiences = grouped.map((role) =>
    toMemberWorkExperience(role, domainsMap.get(role.organizationId) ?? []),
  )

  ok(res, { memberId, workExperiences })
}
