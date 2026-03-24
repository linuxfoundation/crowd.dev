import { WorkflowIdConflictPolicy, WorkflowIdReusePolicy } from '@temporalio/workflow'

import { DEFAULT_TENANT_ID } from '@crowd/common'
import { moveActivityRelationsToAnotherOrganization } from '@crowd/data-access-layer/src/activityRelations'
import { moveInsightsProjectsToAnotherOrganization } from '@crowd/data-access-layer/src/collections'
import {
  deleteOrganizationById,
  deleteOrganizationSegments,
} from '@crowd/data-access-layer/src/old/apps/entity_merging_worker/orgs'
import {
  cleanupForOganization,
  deleteOrganizationAttributes,
  deleteOrganizationEnrichment,
} from '@crowd/data-access-layer/src/organizations'
import { dbStoreQx, pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { SearchSyncApiClient } from '@crowd/opensearch'
import { RedisPubSubEmitter } from '@crowd/redis'
import { ApiWebsocketMessage, TemporalWorkflowId } from '@crowd/types'

import { svc } from '../main'

export async function deleteOrganization(organizationId: string): Promise<void> {
  await deleteOrganizationSegments(svc.postgres.writer, organizationId)

  const qx = dbStoreQx(svc.postgres.writer)
  await deleteOrganizationEnrichment(qx, organizationId)
  await deleteOrganizationAttributes(qx, [organizationId])
  await cleanupForOganization(qx, organizationId)

  await deleteOrganizationById(svc.postgres.writer, organizationId)
}

export async function finishOrganizationMergingUpdateActivities(
  secondaryId: string,
  primaryId: string,
): Promise<void> {
  const qx = pgpQx(svc.postgres.writer.connection())
  await moveActivityRelationsToAnotherOrganization(qx, secondaryId, primaryId)
  await moveInsightsProjectsToAnotherOrganization(qx, secondaryId, primaryId)
}

export async function recalculateActivityAffiliationsOfOrganizationAsync(
  organizationId: string,
): Promise<void> {
  const workflowId = `${TemporalWorkflowId.ORGANIZATION_UPDATE}/${organizationId}`

  try {
    const handle = svc.temporal.workflow.getHandle(workflowId)
    const { status } = await handle.describe()

    if (status.name === 'RUNNING') {
      await handle.result()
    }
  } catch (err) {
    if (err.name !== 'WorkflowNotFoundError') {
      svc.log.error({ err }, 'Failed to check workflow state')
      throw err
    }
  }

  try {
    await svc.temporal.workflow.start('organizationUpdate', {
      taskQueue: 'profiles',
      workflowId,
      workflowIdReusePolicy: WorkflowIdReusePolicy.ALLOW_DUPLICATE,
      workflowIdConflictPolicy: WorkflowIdConflictPolicy.FAIL,
      retry: {
        maximumAttempts: 10,
      },
      args: [
        {
          organization: {
            id: organizationId,
          },
          recalculateAffiliations: true,
          syncOptions: {
            doSync: false,
            withAggs: false,
          },
        },
      ],
    })
  } catch (err) {
    if (err.name === 'WorkflowExecutionAlreadyStartedError') {
      svc.log.info({ workflowId }, 'Workflow already started, skipping')
      return
    }

    throw err
  }
}

export async function syncOrganization(organizationId: string, syncStart: Date): Promise<void> {
  const syncApi = new SearchSyncApiClient({
    baseUrl: process.env['CROWD_SEARCH_SYNC_API_URL'],
  })

  await syncApi.triggerOrganizationSync(organizationId)
  await syncApi.triggerOrganizationMembersSync(organizationId, null, syncStart)
}

export async function notifyFrontendOrganizationUnmergeSuccessful(
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
      'organization-unmerge',
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

export async function notifyFrontendOrganizationMergeSuccessful(
  primaryOrgId: string,
  secondaryOrgId: string,
  original: string,
  toMerge: string,
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
      'org-merge',
      JSON.stringify({
        success: true,
        userId,
        tenantId: DEFAULT_TENANT_ID,
        primaryOrgId,
        secondaryOrgId,
        original,
        toMerge,
      }),
      undefined,
      DEFAULT_TENANT_ID,
    ),
  )
}
