import { Logger } from '@crowd/logging'
import { CrowdQueue, IQueue } from '@crowd/queue'
import { SearchSyncWorkerQueueMessageType } from '@crowd/types'

import { QueuePriorityService } from '../priority.service'

export class SearchSyncWorkerEmitter extends QueuePriorityService {
  public constructor(client: IQueue, parentLog: Logger) {
    super(
      CrowdQueue.SEARCH_SYNC_WORKER,
      client.getQueueChannelConfig(CrowdQueue.SEARCH_SYNC_WORKER),
      client,
      parentLog,
    )
  }

  public async triggerMemberSync(memberId: string, onboarding: boolean, segmentId?: string) {
    if (!memberId) {
      throw new Error('memberId is required!')
    }

    await this.sendMessage(
      memberId,
      {
        type: SearchSyncWorkerQueueMessageType.SYNC_MEMBER,
        memberId,
        segmentId,
      },
      `${memberId}:${segmentId}`,
      {
        onboarding,
      },
    )
  }

  public async triggerMembersSync(
    members: { memberId: string; segmentId?: string }[],
    onboarding: boolean,
  ) {
    if (members.some((m) => !m.memberId)) {
      throw new Error('memberId is required!')
    }

    const messages = members.map(({ memberId, segmentId }) => ({
      payload: {
        type: SearchSyncWorkerQueueMessageType.SYNC_MEMBER,
        memberId,
        segmentId,
      },
      groupId: memberId,
      deduplicationId: `${memberId}:${segmentId}`,
    }))

    await this.sendMessages(messages, { onboarding })
  }

  public async triggerOrganizationMembersSync(organizationId: string, onboarding: boolean) {
    if (!organizationId) {
      throw new Error('organizationId is required!')
    }
    await this.sendMessage(
      organizationId,
      {
        type: SearchSyncWorkerQueueMessageType.SYNC_ORGANIZATION_MEMBERS,
        organizationId,
      },
      undefined,
      {
        onboarding,
      },
    )
  }

  public async triggerRemoveMember(memberId: string, onboarding: boolean) {
    if (!memberId) {
      throw new Error('memberId is required!')
    }

    await this.sendMessage(
      memberId,
      {
        type: SearchSyncWorkerQueueMessageType.REMOVE_MEMBER,
        memberId,
      },
      undefined,
      {
        onboarding,
      },
    )
  }

  public async triggerMemberCleanup() {
    await this.sendMessage(
      'search-sync-worker-system',
      {
        type: SearchSyncWorkerQueueMessageType.CLEANUP_MEMBERS,
      },
      'search-sync-worker-system-cleanup-members',
    )
  }

  public async triggerOrganizationSync(
    organizationId: string,
    onboarding: boolean,
    segmentId?: string,
  ) {
    if (!organizationId) {
      throw new Error('organizationId is required!')
    }

    await this.sendMessage(
      organizationId,
      {
        type: SearchSyncWorkerQueueMessageType.SYNC_ORGANIZATION,
        organizationId,
        segmentId,
      },
      `${organizationId}:${segmentId}`,
      {
        onboarding,
      },
    )
  }

  public async triggerRemoveOrganization(organizationId: string, onboarding: boolean) {
    if (!organizationId) {
      throw new Error('organizationId is required!')
    }

    await this.sendMessage(
      organizationId,
      {
        type: SearchSyncWorkerQueueMessageType.REMOVE_ORGANIZATION,
        organizationId,
      },
      undefined,
      {
        onboarding,
      },
    )
  }

  public async triggerOrganizationCleanup() {
    await this.sendMessage(
      'search-sync-worker-system',
      {
        type: SearchSyncWorkerQueueMessageType.CLEANUP_ORGANIZATIONS,
      },
      'search-sync-worker-system-cleanup-organizations',
    )
  }
}
