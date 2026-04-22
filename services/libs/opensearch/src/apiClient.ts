import axios from 'axios'

export interface ISearchSyncApiConfig {
  baseUrl: string
}

export interface ISyncOrganizationMembersOptions {
  syncFrom?: Date | null
  cursor?: string
  pageSize?: number
  maxPages?: number
  onPageComplete?: (cursor: string, membersSynced: number) => void | Promise<void>
}

export class SearchSyncApiClient {
  private searchSyncApi

  constructor(config: ISearchSyncApiConfig) {
    this.searchSyncApi = axios.create({
      baseURL: config.baseUrl,
    })
  }

  public async triggerMemberSync(memberId: string): Promise<void> {
    if (!memberId) {
      throw new Error('memberId is required!')
    }

    await this.searchSyncApi.post('/sync/members', {
      memberId,
    })
  }

  public async syncOrganizationMembers(
    organizationId: string,
    opts: ISyncOrganizationMembersOptions = {},
  ): Promise<{ totalSynced: number }> {
    if (!organizationId) {
      throw new Error('organizationId is required!')
    }

    const pageSize = opts.pageSize ?? 500
    const maxPages = opts.maxPages ?? 2000

    let cursor = opts.cursor
    let totalSynced = 0

    for (let i = 0; i < maxPages; i++) {
      const { data } = await this.searchSyncApi.post('/sync/organization/members', {
        organizationId,
        lastId: cursor ?? null,
        batchSize: pageSize,
        syncFrom: opts.syncFrom ?? null,
      })

      const result: { lastId: string | null; membersSynced: number } = data
      totalSynced += result.membersSynced

      if (result.lastId === null || result.membersSynced < pageSize) {
        return { totalSynced }
      }

      cursor = result.lastId
      await opts.onPageComplete?.(cursor, result.membersSynced)
    }

    throw new Error(
      `syncOrganizationMembers exceeded maxPages (${maxPages}) for organization ${organizationId}. ` +
        `Synced ${totalSynced} members before aborting.`,
    )
  }

  public async triggerRemoveMember(memberId: string): Promise<void> {
    if (!memberId) {
      throw new Error('memberId is required!')
    }

    await this.searchSyncApi.post('/cleanup/member', {
      memberId,
    })
  }

  public async triggerMemberCleanup(): Promise<void> {
    await this.searchSyncApi.post('/cleanup/members', {})
  }

  public async triggerOrganizationSync(
    organizationId: string,
    segmentIds?: string[],
  ): Promise<void> {
    if (!organizationId) {
      throw new Error('organizationId is required!')
    }

    await this.searchSyncApi.post('/sync/organizations', {
      organizationIds: [organizationId],
      segmentIds,
    })
  }

  public async triggerRemoveOrganization(organizationId: string): Promise<void> {
    if (!organizationId) {
      throw new Error('organizationId is required!')
    }

    await this.searchSyncApi.post('/cleanup/organization', {
      organizationId,
    })
  }

  public async triggerOrganizationCleanup(): Promise<void> {
    await this.searchSyncApi.post('/cleanup/organizations', {})
  }
}
