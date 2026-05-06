import { SearchSyncWorkerEmitter } from '@crowd/common_services'
import { LoggerBase, logExecutionTimeV2 } from '@crowd/logging'
import { SearchSyncApiClient } from '@crowd/opensearch'
import { SyncMode } from '@crowd/types'

import { IS_TEST_ENV } from '@/conf'
import { getSearchSyncWorkerEmitter } from '@/serverless/utils/queueService'

import { getSearchSyncApiClient } from '../utils/apiClients'

import { IServiceOptions } from './IServiceOptions'

export type SearchSyncClient = SearchSyncApiClient | SearchSyncWorkerEmitter

export default class SearchSyncService extends LoggerBase {
  options: IServiceOptions

  mode: SyncMode

  constructor(options: IServiceOptions, mode: SyncMode = SyncMode.ASYNCHRONOUS) {
    super(options.log)
    this.options = options
    this.mode = mode
  }

  async getSearchSyncClient(): Promise<SearchSyncClient> {
    // tests can always use the async emitter
    if (IS_TEST_ENV) {
      return getSearchSyncWorkerEmitter()
    }

    if (this.mode === SyncMode.SYNCHRONOUS) {
      return getSearchSyncApiClient()
    }

    if (this.mode === SyncMode.ASYNCHRONOUS) {
      return getSearchSyncWorkerEmitter()
    }

    throw new Error(`Unknown mode ${this.mode} !`)
  }

  private async logExecutionTime<T>(process: () => Promise<T>, name: string): Promise<T> {
    if (this.options.profileSql) {
      return logExecutionTimeV2(process, this.options.log, name)
    }

    return process()
  }

  async triggerMemberSync(memberId: string) {
    const client = await this.getSearchSyncClient()

    if (client instanceof SearchSyncApiClient) {
      await this.logExecutionTime(
        () => client.triggerMemberSync(memberId),
        `triggerMemberSync: member:${memberId}`,
      )
    } else if (client instanceof SearchSyncWorkerEmitter) {
      await client.triggerMemberSync(memberId, false)
    } else {
      throw new Error('Unexpected search client type!')
    }
  }

  async triggerOrganizationMembersSync(organizationId: string) {
    const client = await this.getSearchSyncClient()

    if (client instanceof SearchSyncApiClient) {
      await this.logExecutionTime(
        () => client.syncOrganizationMembers(organizationId),
        `triggerOrganizationMembersSync: organization:${organizationId}`,
      )
    } else if (client instanceof SearchSyncWorkerEmitter) {
      await client.triggerOrganizationMembersSync(organizationId, false)
    } else {
      throw new Error('Unexpected search client type!')
    }
  }

  async triggerRemoveMember(memberId: string) {
    const client = await this.getSearchSyncClient()

    if (client instanceof SearchSyncApiClient) {
      await this.logExecutionTime(
        () => client.triggerRemoveMember(memberId),
        `triggerRemoveMember: member:${memberId}`,
      )
    } else if (client instanceof SearchSyncWorkerEmitter) {
      await client.triggerRemoveMember(memberId, false)
    } else {
      throw new Error('Unexpected search client type!')
    }
  }

  async triggerMemberCleanup() {
    const client = await this.getSearchSyncClient()

    if (client instanceof SearchSyncApiClient || client instanceof SearchSyncWorkerEmitter) {
      await client.triggerMemberCleanup()
    } else {
      throw new Error('Unexpected search client type!')
    }
  }

  async triggerOrganizationSync(organizationId: string) {
    const client = await this.getSearchSyncClient()

    if (client instanceof SearchSyncApiClient) {
      await this.logExecutionTime(
        () => client.triggerOrganizationSync(organizationId),
        `triggerOrganizationSync: organization:${organizationId}`,
      )
    } else if (client instanceof SearchSyncWorkerEmitter) {
      await client.triggerOrganizationSync(organizationId, false)
    } else {
      throw new Error('Unexpected search client type!')
    }
  }

  async triggerRemoveOrganization(organizationId: string) {
    const client = await this.getSearchSyncClient()

    if (client instanceof SearchSyncApiClient) {
      await this.logExecutionTime(
        () => client.triggerRemoveOrganization(organizationId),
        `triggerRemoveOrganization: organization:${organizationId}`,
      )
    } else if (client instanceof SearchSyncWorkerEmitter) {
      await client.triggerRemoveOrganization(organizationId, false)
    } else {
      throw new Error('Unexpected search client type!')
    }
  }

  async triggerOrganizationCleanup() {
    const client = await this.getSearchSyncClient()

    if (client instanceof SearchSyncApiClient || client instanceof SearchSyncWorkerEmitter) {
      await client.triggerOrganizationCleanup()
    } else {
      throw new Error('Unexpected search client type!')
    }
  }
}
