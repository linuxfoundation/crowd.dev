import { OrganizationIdentityType, OrganizationSource } from './enums/organizations'
import { IMemberOrganizationAffiliationOverride } from './members'

export interface IOrganization {
  // db fields
  id?: string
  url?: string
  avatarUrl?: string
  importHash?: string
  createdAt?: string | Date
  updatedAt?: string | Date
  deletedAt?: string | Date
  tenantId?: string
  createdById?: string
  updatedById?: string
  isTeamOrganization?: boolean
  isAffiliationBlocked?: boolean
  lastEnrichedAt?: string | Date
  searchSyncedAt?: string | Date
  manuallyCreated?: boolean

  // calculated fields
  revenueRangeMin?: number
  revenueRangeMax?: number
  source?: OrganizationSource
  employeeChurnRate12Month?: number
  employeeGrowthRate12Month?: number

  // relations
  identities?: IOrganizationIdentity[]
  members?: string[]

  // attributes
  tags?: string[]
  description?: string
  logo?: string
  headline?: string
  employees?: number
  revenueRange?: IOrganizationRevenueRange
  location?: string
  type?: string
  size?: string
  industry?: string
  founded?: number
  displayName?: string
  employeeChurnRate?: Record<string, number>
  employeeGrowthRate?: Record<string, number>

  attributes?: any // eslint-disable-line @typescript-eslint/no-explicit-any
}

export interface IMemberOrganization {
  id?: string
  title?: string
  dateStart: Date | string
  dateEnd: Date | string
  memberId: string
  organizationId: string
  updatedAt?: string
  createdAt?: string
  source?: string
  verified?: boolean
  verifiedBy?: string
  deletedAt?: string
  displayName?: string
  affiliationOverride?: IMemberOrganizationAffiliationOverride
}

type MemberOrganizationEditableFields = Pick<
  IMemberOrganization,
  | 'organizationId'
  | 'memberId'
  | 'title'
  | 'dateStart'
  | 'dateEnd'
  | 'source'
  | 'verified'
  | 'verifiedBy'
>

export type MemberOrganizationUpdate = Partial<MemberOrganizationEditableFields>

export interface IRenderFriendlyMemberOrganization {
  id: string
  displayName?: string
  logo?: string
  memberOrganizations: IMemberOrganization
}

export interface IMemberRoleWithOrganization extends IMemberOrganization {
  organizationName: string
  organizationLogo: string
}

export interface IExecutiveChange {
  joined_date?: string
  pdl_id?: string
  job_title?: string
  job_title_role?: string
  job_title_sub_role?: string
  job_title_levels?: string[]
  new_company_id?: string
  new_company_job_title?: string
  new_company_job_title_role?: string
  new_company_job_title_sub_role?: string
  new_company_job_title_levels?: string[]
}

export interface IOrganizationSocial {
  handle: string
  url?: string
}

export interface IOrganizationSyncRemoteData {
  id?: string
  organizationId: string
  sourceId?: string
  integrationId: string
  syncFrom: string
  metaData: string
  lastSyncedAt?: string
}

export interface NewOrganizationIdentity {
  organizationId: string
  platform: string
  value: string
  type: OrganizationIdentityType
  verified: boolean
  source: string
  sourceId?: string | null
  integrationId?: string | null
}

export interface IOrganizationIdentity {
  organizationId?: string
  platform: string
  value: string
  type: OrganizationIdentityType
  verified: boolean
  source?: string
  sourceId?: string
  integrationId?: string
}

export interface IOrganizationMergeSuggestion {
  similarity: number
  organizations: [string, string]
}
export interface IOrganizationIdSource {
  id: string
  source: string
}

export interface IOrganizationRevenueRange {
  min: number
  max: number
}

export interface IOrganizationNaics {
  sector: string
  naics_code: string
  sub_sector: string
  industry_group: string
  naics_industry: string
  national_industry: string
}

export interface IOrganizationAddress {
  name: string
  metro: string
  region: string
  country: string
  locality: string
  continent: string
  postal_code: string
  address_line_2: string
  street_address: string
}

export interface ILLMConsumableOrganizationDbResult {
  displayName: string
  description: string
  phoneNumbers: string[]
  logo: string
  tags: string[]
  location: string
  type: string
  geoLocation: string
  ticker: string
  profiles: string[]
  headline: string
  industry: string
  founded: number
  alternativeNames: string[]
  identities: IOrganizationIdentity[]
}

export interface ILLMConsumableOrganization {
  displayName: string
  description: string
  phoneNumbers: string[]
  logo: string
  tags: string[]
  location: string
  type: string
  geoLocation: string
  ticker: string
  profiles: string[]
  headline: string
  industry: string
  founded: number
  alternativeNames: string[]
  identities: {
    platform: string
    value: string
  }[]
}

export interface IOrganizationOpensearch {
  uuid_organizationId: string
  uuid_tenantId?: string
  keyword_displayName: string
  nested_identities: IOrganizationIdentityOpensearch[]
  string_location: string
  string_industry: string
  string_website: string
  string_ticker: string
  int_activityCount: number
}

export interface IOrganizationIdentityOpensearch {
  string_platform: string
  string_type: string
  keyword_type: string
  string_value: string
  bool_verified: boolean
  string_source: string
}

export interface IOrganizationFullAggregatesOpensearch
  extends IOrganizationBaseForMergeSuggestions {
  ticker: string
  identities: IOrganizationIdentity[]
  activityCount: number
  noMergeIds: string[]
  website: string
}

export interface IDbOrganizationSyncData {
  // base
  organizationId: string
  displayName: string

  ticker: string | null
  industry: string | null
  location: string | null

  activityCount: number

  identities: IOrganizationIdentity[]
}

export interface IOrganizationBaseForMergeSuggestions {
  id: string
  displayName: string
  location: string
  industry: string
}
