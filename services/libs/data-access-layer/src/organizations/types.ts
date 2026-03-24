import {
  IOrganizationRevenueRange,
  OrganizationAttributeSource,
  OrganizationIdentityType,
  PlatformType,
} from '@crowd/types'

export interface IDbOrganization {
  id: string
  description?: string
  displayName: string
  logo?: string
  tags?: string[]
  employees?: number
  revenueRange?: IOrganizationRevenueRange
  importHash?: string
  location?: string
  isTeamOrganization: boolean
  isAffiliationBlocked?: boolean
  type?: string
  size?: string
  headline?: string
  industry?: string
  founded?: number
  employeeChurnRate?: unknown
  employeeGrowthRate?: unknown
  manuallyCreated: boolean
}

export interface IDbOrganizationInput {
  description?: string
  displayName?: string
  logo?: string
  tags?: string[]
  employees?: number
  revenueRange?: IOrganizationRevenueRange
  importHash?: string
  location?: string
  isTeamOrganization: boolean
  isAffiliationBlocked?: boolean
  type?: string
  size?: string
  headline?: string
  industry?: string
  founded?: number
  employeeChurnRate?: unknown
  employeeGrowthRate?: unknown
  manuallyCreated: boolean
}

export interface IDbOrgAttribute {
  id?: string
  name: string
  source: OrganizationAttributeSource
  default: boolean
  value: string
}

export interface IDbOrgAttributeInput {
  name: string
  source: string
  default: boolean
  value: string
}

export interface IDbOrganizationAggregateData
  extends IOrganizationActivityCoreAggregates,
    IOrganizationDisplayAggregates {}

export interface IOrganizationActivityCoreAggregates {
  organizationId: string
  segmentId: string

  memberCount: number
  activityCount: number
  activeOn: string[]
}

export interface IOrganizationDisplayAggregates {
  organizationId: string
  segmentId: string

  joinedAt: string
  lastActive: string
  avgContributorEngagement: number
}

export interface IDbOrgIdentity {
  platform: string
  type: OrganizationIdentityType
  value: string
  verified: boolean
  sourceId?: string
  integrationId?: string
}

export interface IDbOrgIdentityUpdateInput {
  organizationId: string
  platform: string
  value: string
  type: OrganizationIdentityType
  verified: boolean
}

export const ENRICHMENT_PLATFORM_PRIORITY = [
  PlatformType.GITHUB,
  PlatformType.LINKEDIN,
  PlatformType.TWITTER,
]

export interface IEnrichableOrganizationData {
  organizationId: string
}

export interface IQueryNumberOfNewOrganizations {
  segmentIds?: string[]
  after?: Date
  before?: Date
  platform?: string
}

export interface IQueryTimeseriesOfNewOrganizations {
  segmentIds?: string[]
  after: Date
  before: Date
  platform?: string
}

export interface IQueryNumberOfActiveOrganizations {
  segmentIds?: string[]
  after: Date
  before: Date
  platform?: string
}
