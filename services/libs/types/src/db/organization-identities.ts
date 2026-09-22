import type { OrganizationIdentityType } from '../enums/organizations'

export interface OrganizationIdentityDbRow {
  organizationId: string
  platform: string
  value: string
  type: OrganizationIdentityType
  verified: boolean
  source: string | null
  sourceId: string | null
  integrationId: string | null
  tenantId: string
  createdAt: string
  updatedAt: string
}

export type OrganizationIdentityDbInsert = Pick<
  OrganizationIdentityDbRow,
  'organizationId' | 'platform' | 'value' | 'type' | 'verified'
> &
  Partial<Pick<OrganizationIdentityDbRow, 'source' | 'sourceId' | 'integrationId'>>
