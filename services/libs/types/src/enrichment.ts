import { MemberEnrichmentSource, OrganizationEnrichmentSource } from './enums'
import { IMemberIdentity, IMemberReach } from './members'
import { IOrganizationIdentity } from './organizations'

export interface IMemberEnrichmentCache<T> {
  createdAt: string
  updatedAt: string
  memberId: string
  data: T
  source: MemberEnrichmentSource
}

export interface IEnrichmentSourceQueryInput<T> {
  source: T
  cacheObsoleteAfterSeconds?: number
  enrichableBySql: string
  neverReenrich?: boolean
}

export interface IEnrichableMember {
  id: string
  displayName: string
  location: string
  website: string
  identities: IMemberIdentity[]
  activityCount: number
}

export interface IEnrichableMemberIdentityActivityAggregate {
  activityCount: number
  username: string
  platform: string
}

export interface IMemberOrganizationData {
  id: string
  orgId: string
  orgName?: string
  jobTitle: string
  dateStart: string
  dateEnd: string
  source: string
  verified?: boolean
  verifiedBy?: string | null
  identities?: IOrganizationIdentity[]
}

export interface IDeletedMemberOrganizationData {
  orgId: string
}

export interface IMemberOriginalData {
  // members table data
  displayName: string
  attributes: Record<string, Record<string, unknown>>
  manuallyChangedFields: string[]
  reach: IMemberReach

  // memberIdentities table data
  identities: IMemberIdentity[]

  // memberOrganizations table data
  organizations: IMemberOrganizationData[]
  // memberOrganizations rows manually deleted by a person (deletedBy set). Optional because
  // in-flight Temporal histories may carry a pre-rollout result that predates this field.
  deletedOrganizations?: IDeletedMemberOrganizationData[]
}

export interface IOrganizationEnrichmentCache<T> {
  createdAt: string
  updatedAt: string
  organizationId: string
  data: T
  source: OrganizationEnrichmentSource
}

export interface IEnrichableOrganization {
  id: string
  displayName: string
  identities: IOrganizationIdentity[]
  activityCount: number
}
