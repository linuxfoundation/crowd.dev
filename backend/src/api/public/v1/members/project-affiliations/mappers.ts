import type {
  ISegmentAffiliationWithOrg,
  IWorkExperienceAffiliation,
} from '@crowd/data-access-layer'

export const AFFILIATION_TYPE = {
  PROJECT: 'project',
  WORK_HISTORY: 'work-history',
} as const

export type AffiliationType = (typeof AFFILIATION_TYPE)[keyof typeof AFFILIATION_TYPE]

export function mapSegmentAffiliation(a: ISegmentAffiliationWithOrg) {
  return {
    id: a.id,
    organizationId: a.organizationId,
    organizationName: a.organizationName,
    organizationLogo: a.organizationLogo ?? null,
    verified: a.verified,
    verifiedBy: a.verifiedBy ?? null,
    startDate: a.dateStart ?? null,
    endDate: a.dateEnd ?? null,
    type: AFFILIATION_TYPE.PROJECT,
  }
}

export function mapWorkExperienceAffiliation(a: IWorkExperienceAffiliation) {
  return {
    id: a.id,
    organizationId: a.organizationId,
    organizationName: a.organizationName,
    organizationLogo: a.organizationLogo ?? null,
    verified: a.verified ?? false,
    verifiedBy: a.verifiedBy ?? null,
    source: a.source ?? null,
    startDate: a.dateStart ?? null,
    endDate: a.dateEnd ?? null,
    type: AFFILIATION_TYPE.WORK_HISTORY,
  }
}
