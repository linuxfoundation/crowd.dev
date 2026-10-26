import { IOrganizationIdentity, OrganizationEnrichmentSource } from '@crowd/types'

import { IOrganizationEnrichmentDataInternalAPI } from './sources/lfx-internal-api/types'

export interface IOrganizationEnrichmentSourceInput {
  organizationId: string
  domains: IOrganizationIdentity[]
  displayName: string
  activityCount: number
}

export type IOrganizationEnrichmentData = IOrganizationEnrichmentDataInternalAPI

export interface IOrganizationEnrichmentService {
  source: OrganizationEnrichmentSource

  // cache rows with older updatedAt than this will be considered obsolete and will be re-enriched
  // not required when neverReenrich is true
  cacheObsoleteAfterSeconds?: number

  // when true, organizations are enriched only once and cached data is never refreshed
  neverReenrich?: boolean

  // max concurrent requests that can be made to the source
  maxConcurrentRequests: number

  // can the source enrich using this input
  isEnrichableBySource(input: IOrganizationEnrichmentSourceInput): Promise<boolean>

  // does the source have credits to enrich organizations, if returned false the source will be skipped
  // response will be saved to redis for 60 seconds and will be used for subsequent calls
  hasRemainingCredits(): Promise<boolean>

  // SQL filter to get enrichable organizations for a source
  enrichableBySql: string

  // only enrich organizations with activity more than this number
  enrichOrganizationsWithActivityMoreThan?: number

  // should either return the data or null if it's a miss
  getData(input: IOrganizationEnrichmentSourceInput): Promise<IOrganizationEnrichmentData | null>

  normalize(data: IOrganizationEnrichmentData): IOrganizationEnrichmentDataNormalized
}

export interface IOrganizationEnrichmentDataNormalized {
  identities?: IOrganizationIdentity[]
  attributes?: Record<string, unknown>
  displayName?: string
}

export interface ITriggerOrganizationsEnrichmentInput {
  testRun?: boolean
  perRunLimit?: number
}
