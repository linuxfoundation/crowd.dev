import type { IMemberRoleWithOrganization } from '@crowd/types'

export function toMemberWorkExperience(mo: IMemberRoleWithOrganization) {
  return {
    id: mo.id,
    organizationId: mo.organizationId,
    organizationName: mo.organizationName,
    organizationLogo: mo.organizationLogo,
    jobTitle: mo.title ?? null,
    verified: mo.verified ?? false,
    verifiedBy: mo.verifiedBy ?? null,
    source: mo.source ?? null,
    startDate: mo.dateStart ?? null,
    endDate: mo.dateEnd ?? null,
    createdAt: mo.createdAt ?? null,
    updatedAt: mo.updatedAt ?? null,
  }
}
