import type { MemberIdentityType } from '../enums/members'

export interface MemberIdentityDbRow {
  id: string
  memberId: string
  tenantId: string
  platform: string
  value: string
  type: MemberIdentityType
  verified: boolean
  verifiedBy: string | null
  source: string | null
  sourceId: string | null
  integrationId: string | null
  createdAt: string
  updatedAt: string
  deletedAt: string | null
}

export type MemberIdentityDbInsert = Pick<
  MemberIdentityDbRow,
  'memberId' | 'platform' | 'value' | 'type'
> &
  Partial<
    Omit<
      MemberIdentityDbRow,
      | 'memberId'
      | 'platform'
      | 'value'
      | 'type'
      | 'tenantId'
      | 'createdAt'
      | 'updatedAt'
      | 'deletedAt'
    >
  >
