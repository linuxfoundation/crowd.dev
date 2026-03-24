import {
  IAttributes,
  IMemberContribution,
  IMemberIdentity,
  IMemberReach,
  IOrganizationIdentity,
  MemberAttributeName,
  MemberEnrichmentSource,
  NewMemberIdentity,
  OrganizationSource,
  PlatformType,
} from '@crowd/types'

import { IMemberEnrichmentDataClearbit } from './sources/clearbit/types'
import { IMemberEnrichmentDataCrustdata } from './sources/crustdata/types'
import { IMemberEnrichmentDataProgAILinkedinScraper } from './sources/progai-linkedin-scraper/types'
import { IMemberEnrichmentDataProgAI } from './sources/progai/types'
import { IMemberEnrichmentDataSerp } from './sources/serp/types'

/* eslint-disable @typescript-eslint/no-explicit-any */

export interface IEnrichmentSourceInput {
  memberId: string
  github?: IMemberIdentity
  linkedin?: IMemberIdentity
  email?: IMemberIdentity
  website?: string
  location?: string
  displayName?: string
  activityCount?: number
}

export type IMemberEnrichmentData =
  | IMemberEnrichmentDataProgAI
  | IMemberEnrichmentDataClearbit
  | IMemberEnrichmentDataSerp
  | IMemberEnrichmentDataProgAILinkedinScraper[]
  | IMemberEnrichmentDataCrustdata
  | IMemberEnrichmentDataCrustdata[]

export interface IEnrichmentService {
  source: MemberEnrichmentSource

  // cache rows with older updatedAt than this will be considered obsolete and will be re-enriched
  // not required when neverReenrich is true
  cacheObsoleteAfterSeconds?: number

  // when true, members are enriched only once and cached data is never refreshed
  neverReenrich?: boolean

  // max concurrent requests that can be made to the source
  maxConcurrentRequests: number

  // can the source enrich using this input
  isEnrichableBySource(input: IEnrichmentSourceInput): Promise<boolean>

  // does the source have credits to enrich members, if returned false the source will be skipped
  // response will be saved to redis for 60 seconds and will be used for subsequent calls
  hasRemainingCredits(): Promise<boolean>

  // SQL filter to get enrichable members for a source
  // members table is available as "members" alias
  // memberIdentities table is available as "mi" alias
  // activity count is available in "membersGlobalActivityCount" alias, "membersGlobalActivityCount".total_count field
  enrichableBySql: string

  // only enrich members with activity more than this number
  enrichMembersWithActivityMoreThan?: number

  // should either return the data or null if it's a miss
  getData(input: IEnrichmentSourceInput): Promise<IMemberEnrichmentData | null>
  normalize(
    data: IMemberEnrichmentData,
  ): IMemberEnrichmentDataNormalized | IMemberEnrichmentDataNormalized[]
}

export type IMemberEnrichmentMetadataNormalized = IMemberEnrichmentLinkedinScraperMetadata

export interface IMemberEnrichmentDataNormalized {
  identities?: Omit<NewMemberIdentity, 'memberId'>[]
  contributions?: IMemberContribution[]
  attributes?: IAttributes
  reach?: IMemberReach
  memberOrganizations?: IMemberEnrichmentDataNormalizedOrganization[]
  displayName?: string
  metadata?: IMemberEnrichmentMetadataNormalized
}

export interface IMemberEnrichmentDataNormalizedOrganization {
  id?: string
  organizationId?: string
  name: string
  identities?: IOrganizationIdentity[]
  title?: string
  organizationDescription?: string
  startDate?: string
  endDate?: string
  source: OrganizationSource
}

export interface IMemberEnrichmentLinkedinScraperMetadata {
  repeatedTimesInDifferentSources: number
  isFromVerifiedSource: boolean
}

export interface IMemberEnrichmentSocialData {
  platform: PlatformType
  handle: string
}

export type IMemberEnrichmentAttributeSettings = {
  [key in MemberAttributeName]?: {
    fields: string[]
    transform?: (data: unknown) => unknown
  }
}

export interface IProcessMemberSourcesArgs {
  memberId: string
  sources: MemberEnrichmentSource[]
}

type MemberIdentityConsumableBase = Omit<
  IMemberIdentity,
  'id' | 'memberId' | 'createdAt' | 'updatedAt' | 'deletedAt'
>

export type ConsumableIdentity = MemberIdentityConsumableBase & {
  repeatedTimesInDifferentSources: number
  isFromVerifiedSource: boolean
}
