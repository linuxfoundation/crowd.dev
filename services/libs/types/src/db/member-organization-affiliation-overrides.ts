export interface MemberOrganizationAffiliationOverrideDbRow {
  id: string
  memberId: string
  memberOrganizationId: string
  allowAffiliation: boolean | null
  isPrimaryWorkExperience: boolean | null
}

export type MemberOrganizationAffiliationOverrideDbInsert = Pick<
  MemberOrganizationAffiliationOverrideDbRow,
  'memberId' | 'memberOrganizationId'
> &
  Partial<Omit<MemberOrganizationAffiliationOverrideDbRow, 'memberId' | 'memberOrganizationId'>>
