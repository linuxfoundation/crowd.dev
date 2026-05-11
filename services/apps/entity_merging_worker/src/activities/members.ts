import { WorkflowIdConflictPolicy } from '@temporalio/workflow'

import { DEFAULT_TENANT_ID } from '@crowd/common'
import {
  moveActivityRelationsToAnotherMember,
  moveActivityRelationsWithIdentityToAnotherMember,
} from '@crowd/data-access-layer/src/activityRelations'
import { cleanupMemberAggregates } from '@crowd/data-access-layer/src/members/segments'
import {
  cleanupMember,
  deleteMemberSegments,
} from '@crowd/data-access-layer/src/old/apps/entity_merging_worker'
import { dbStoreQx, pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { SearchSyncApiClient } from '@crowd/opensearch'
import { RedisPubSubEmitter } from '@crowd/redis'
import { ApiWebsocketMessage, IMemberIdentity, TemporalWorkflowId } from '@crowd/types'

import { svc } from '../main'

export async function deleteMember(memberId: string): Promise<void> {
  await deleteMemberSegments(svc.postgres.writer, memberId)
  const qx = dbStoreQx(svc.postgres.writer)
  await cleanupMemberAggregates(qx, memberId)
  await cleanupMember(svc.postgres.writer, memberId)
}

export async function recalculateActivityAffiliationsOfMemberAsync(
  memberId: string,
): Promise<void> {
  await svc.temporal.workflow.signalWithStart('memberUpdate', {
    taskQueue: 'profiles',
    workflowId: `${TemporalWorkflowId.MEMBER_UPDATE}/${memberId}`,
    workflowIdConflictPolicy: WorkflowIdConflictPolicy.USE_EXISTING,
    signal: 'refreshAffiliations',
    signalArgs: [{ member: { id: memberId }, memberOrganizationIds: [], syncToOpensearch: false }],
    retry: {
      maximumAttempts: 10,
    },
    args: [],
    searchAttributes: {
      TenantId: [DEFAULT_TENANT_ID],
    },
  })
}

export async function syncMember(memberId: string): Promise<void> {
  const syncApi = new SearchSyncApiClient({
    baseUrl: process.env['CROWD_SEARCH_SYNC_API_URL'],
  })

  await syncApi.triggerMemberSync(memberId)
}

export async function syncRemoveMember(memberId: string): Promise<void> {
  const syncApi = new SearchSyncApiClient({
    baseUrl: process.env['CROWD_SEARCH_SYNC_API_URL'],
  })
  await syncApi.triggerRemoveMember(memberId)
}

export async function notifyFrontendMemberMergeSuccessful(
  primaryId: string,
  secondaryId: string,
  primaryDisplayName: string,
  secondaryDisplayName: string,
  userId: string,
): Promise<void> {
  const emitter = new RedisPubSubEmitter(
    'api-pubsub',
    svc.redis,
    (err) => {
      svc.log.error({ err }, 'Error in api-ws emitter!')
    },
    svc.log,
  )

  emitter.emit(
    'user',
    new ApiWebsocketMessage(
      'member-merge',
      JSON.stringify({
        success: true,
        tenantId: DEFAULT_TENANT_ID,
        userId,
        primaryId,
        secondaryId,
        primaryDisplayName,
        secondaryDisplayName,
      }),
      undefined,
      DEFAULT_TENANT_ID,
    ),
  )
}

export async function notifyFrontendMemberUnmergeSuccessful(
  primaryId: string,
  secondaryId: string,
  primaryDisplayName: string,
  secondaryDisplayName: string,
  userId: string,
): Promise<void> {
  const emitter = new RedisPubSubEmitter(
    'api-pubsub',
    svc.redis,
    (err) => {
      svc.log.error({ err }, 'Error in api-ws emitter!')
    },
    svc.log,
  )

  emitter.emit(
    'user',
    new ApiWebsocketMessage(
      'member-unmerge',
      JSON.stringify({
        success: true,
        tenantId: DEFAULT_TENANT_ID,
        userId,
        primaryId,
        secondaryId,
        primaryDisplayName,
        secondaryDisplayName,
      }),
      undefined,
      DEFAULT_TENANT_ID,
    ),
  )
}

export async function finishMemberMergingUpdateActivities(secondaryId: string, primaryId: string) {
  const qx = pgpQx(svc.postgres.writer.connection())
  await moveActivityRelationsToAnotherMember(qx, secondaryId, primaryId)
}

export async function finishMemberUnmergingUpdateActivities(
  primaryId: string,
  secondaryId: string,
  identities: IMemberIdentity[],
) {
  const qx = pgpQx(svc.postgres.writer.connection())

  await Promise.all(
    identities.map((identity) =>
      moveActivityRelationsWithIdentityToAnotherMember(
        qx,
        primaryId,
        secondaryId,
        identity.value,
        identity.platform,
      ),
    ),
  )
}
