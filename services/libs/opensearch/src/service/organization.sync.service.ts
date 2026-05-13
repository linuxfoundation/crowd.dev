import { DEFAULT_TENANT_ID } from '@crowd/common'
import { OrganizationField, findOrgById } from '@crowd/data-access-layer'
import { findOrgNoMergeIds } from '@crowd/data-access-layer/src/org_merge'
import { IOrganizationActivityCoreAggregates } from '@crowd/data-access-layer/src/organizations'
import { findOrgAttributes } from '@crowd/data-access-layer/src/organizations/attributes'
import { fetchOrgIdentities } from '@crowd/data-access-layer/src/organizations/identities'
import { fetchTotalActivityCount } from '@crowd/data-access-layer/src/organizations/segments'
import { QueryExecutor, repoQx } from '@crowd/data-access-layer/src/queryExecutor'
import { DbStore } from '@crowd/database'
import { Logger, getChildLogger } from '@crowd/logging'
import {
  IOrganizationBaseForMergeSuggestions,
  IOrganizationFullAggregatesOpensearch,
  IOrganizationOpensearch,
  OpenSearchIndex,
  OrganizationIdentityType,
} from '@crowd/types'

import { OrganizationRepository } from '../repo/organization.repo'

import { ISearchHit } from './opensearch.data'
import { OpenSearchService } from './opensearch.service'
import { IOrganizationSyncResult } from './organization.sync.data'

export async function buildFullOrgForMergeSuggestions(
  qx: QueryExecutor,
  organization: IOrganizationBaseForMergeSuggestions,
): Promise<IOrganizationFullAggregatesOpensearch> {
  const identities = await fetchOrgIdentities(qx, organization.id)
  const attributes = await findOrgAttributes(qx, organization.id)
  const totalActivityCount = await fetchTotalActivityCount(qx, organization.id)
  const noMergeIds = await findOrgNoMergeIds(qx, organization.id)

  return {
    ...organization,
    ticker: attributes.find((a) => a.name === 'ticker' && a.default)?.value,
    identities,
    activityCount: totalActivityCount,
    noMergeIds,
    website: identities.find((i) => i.type === OrganizationIdentityType.PRIMARY_DOMAIN)?.value,
  }
}

/* eslint-disable @typescript-eslint/no-explicit-any */

export class OrganizationSyncService {
  private log: Logger
  private readonly readOrgRepo: OrganizationRepository

  constructor(
    writeStore: DbStore,
    private readonly openSearchService: OpenSearchService,
    parentLog: Logger,
    readStore?: DbStore,
  ) {
    this.log = getChildLogger('organization-sync-service', parentLog)

    this.readOrgRepo = new OrganizationRepository(readStore || writeStore, this.log)
  }

  public async cleanupOrganizationIndex(batchSize = 300): Promise<void> {
    this.log.warn('Cleaning up organization index!')

    const query = {
      bool: {
        filter: {
          term: {
            uuid_tenantId: DEFAULT_TENANT_ID,
          },
        },
      },
    }

    const sort = [{ date_createdAt: 'asc' }]
    const include = ['date_createdAt', 'uuid_organizationId']
    const pageSize = 500
    let lastCreatedAt: string

    let results = (await this.openSearchService.search(
      OpenSearchIndex.ORGANIZATIONS,
      query,
      undefined,
      pageSize,
      sort,
      undefined,
      include,
    )) as ISearchHit<{ date_createdAt: string; uuid_organizationId: string }>[]

    let processed = 0
    const idsToRemove: string[] = []

    while (results.length > 0) {
      // check every organization if they exists in the database and if not remove them from the index
      const dbIds = await this.readOrgRepo.checkOrganizationsExists(
        results.map((r) => r._source.uuid_organizationId),
      )

      const toRemove = results
        .filter((r) => !dbIds.includes(r._source.uuid_organizationId))
        .map((r) => r._id)

      idsToRemove.push(...toRemove)

      // Process bulk removals in chunks
      while (idsToRemove.length >= batchSize) {
        const batch = idsToRemove.splice(0, batchSize)
        this.log.warn({ batch }, 'Removing organizations from index!')
        await this.openSearchService.bulkRemoveFromIndex(batch, OpenSearchIndex.ORGANIZATIONS)
      }

      processed += results.length
      this.log.warn(`Processed ${processed} organizations while cleaning up!`)

      // use last createdAt to get the next page
      lastCreatedAt = results[results.length - 1]._source.date_createdAt
      results = (await this.openSearchService.search(
        OpenSearchIndex.ORGANIZATIONS,
        query,
        undefined,
        pageSize,
        sort,
        lastCreatedAt,
        include,
      )) as ISearchHit<{ date_createdAt: string; uuid_organizationId: string }>[]
    }

    // Remove any remaining IDs that were not processed
    if (idsToRemove.length > 0) {
      this.log.warn({ idsToRemove }, 'Removing remaining organizations from index!')
      await this.openSearchService.bulkRemoveFromIndex(idsToRemove, OpenSearchIndex.ORGANIZATIONS)
    }

    this.log.warn(`Processed total of ${processed} organizations while cleaning up!`)
  }

  public async removeOrganization(organizationId: string): Promise<void> {
    this.log.debug({ organizationId }, 'Removing organization from index!')

    const query = {
      bool: {
        filter: {
          term: {
            uuid_organizationId: organizationId,
          },
        },
      },
    }

    const sort = [{ _id: 'asc' }]
    const pageSize = 10
    let lastId: string

    let results = (await this.openSearchService.search(
      OpenSearchIndex.ORGANIZATIONS,
      query,
      undefined,
      pageSize,
      sort,
      undefined,
      undefined,
    )) as ISearchHit<object>[]

    while (results.length > 0) {
      const ids = results.map((r) => r._id)
      await this.openSearchService.bulkRemoveFromIndex(ids, OpenSearchIndex.ORGANIZATIONS)

      // use last _id to get the next page
      lastId = results[results.length - 1]._id
      results = (await this.openSearchService.search(
        OpenSearchIndex.ORGANIZATIONS,
        query,
        undefined,
        pageSize,
        sort,
        lastId,
        undefined,
      )) as ISearchHit<object>[]
    }
  }

  public async syncOrganizations(organizationIds: string[]): Promise<IOrganizationSyncResult> {
    const syncOrgsToOpensearchForMergeSuggestions = async (organizationIds) => {
      let indexed = 0
      for (const orgId of organizationIds) {
        const qx = repoQx(this.readOrgRepo)
        const base = await findOrgById(qx, orgId, [
          OrganizationField.ID,
          OrganizationField.DISPLAY_NAME,
          OrganizationField.LOCATION,
          OrganizationField.INDUSTRY,
        ])

        if (base) {
          const data = await buildFullOrgForMergeSuggestions(qx, base)
          const prefixed = OrganizationSyncService.prefixData(data)
          await this.openSearchService.index(orgId, OpenSearchIndex.ORGANIZATIONS, prefixed)
          indexed++
        }
      }

      return indexed
    }

    const indexed = await syncOrgsToOpensearchForMergeSuggestions(organizationIds)

    return {
      documentsIndexed: indexed,
    }
  }

  private static aggregateData(
    segmentId: string,
    input: IOrganizationActivityCoreAggregates[],
  ): IOrganizationActivityCoreAggregates {
    const activityCount = input.reduce((sum, data) => sum + data.activityCount, 0)
    const activeOn = [...new Set(input.flatMap((data) => data.activeOn))]
    const memberCount = input.reduce((sum, data) => sum + data.memberCount, 0)

    return {
      organizationId: input[0].organizationId,
      segmentId,

      activeOn,
      activityCount,
      memberCount,
    }
  }

  public static prefixData(data: IOrganizationFullAggregatesOpensearch): IOrganizationOpensearch {
    return {
      uuid_organizationId: data.id,
      uuid_tenantId: DEFAULT_TENANT_ID,
      string_location: data.location,
      string_industry: data.industry,
      string_ticker: data.ticker,
      keyword_displayName: data.displayName,
      string_website: data.website,

      nested_identities: data.identities.map((identity) => ({
        string_platform: identity.platform,
        string_value: identity.value,
        keyword_type: identity.type,
        string_type: identity.type,
        bool_verified: identity.verified,
        string_source: identity.source ?? '',
      })),

      int_activityCount: data.activityCount,
    }
  }
}
