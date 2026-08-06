export interface MemberOrganizationDbRow {
  id: string
  memberId: string
  organizationId: string
  dateStart: string | null
  dateEnd: string | null
  title: string | null
  source: string | null
  verified: boolean
  verifiedBy: string | null
  createdAt: string
  updatedAt: string
  deletedAt: string | null
}

export type MemberOrganizationDbInsert = Pick<
  MemberOrganizationDbRow,
  'memberId' | 'organizationId'
> &
  Partial<
    Omit<
      MemberOrganizationDbRow,
      'memberId' | 'organizationId' | 'createdAt' | 'updatedAt' | 'deletedAt'
    >
  >
