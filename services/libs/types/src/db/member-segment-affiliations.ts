export interface MemberSegmentAffiliationDbRow {
  id: string
  memberId: string
  segmentId: string
  organizationId: string | null
  dateStart: string | null
  dateEnd: string | null
  verified: boolean
  verifiedBy: string | null
  deletedAt: string | null
}

export type MemberSegmentAffiliationDbInsert = Pick<
  MemberSegmentAffiliationDbRow,
  'memberId' | 'segmentId'
> &
  Partial<Omit<MemberSegmentAffiliationDbRow, 'memberId' | 'segmentId' | 'deletedAt'>>
