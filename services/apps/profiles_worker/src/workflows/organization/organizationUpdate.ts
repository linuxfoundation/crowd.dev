import {
  ParentClosePolicy,
  WorkflowIdConflictPolicy,
  continueAsNew,
  proxyActivities,
  startChild,
} from '@temporalio/workflow'

import { DEFAULT_TENANT_ID } from '@crowd/common'
import { TemporalWorkflowId } from '@crowd/types'

import * as activities from '../../activities'
import { MemberUpdateInput } from '../../types/member'
import { IOrganizationProfileSyncInput } from '../../types/organization'

// Configure timeouts and retry policies to update a member in the database.
const { syncOrganization, findMembersInOrganization } = proxyActivities<typeof activities>({
  startToCloseTimeout: '5 minutes',
})

/*
organizationUpdate can do the following:
  - Update all affiliations of members in a given organization in the database, if recalculateAffiliations is true.
  - Sync organization to OpenSearch, if syncOptions.doSync is true.
  - Also sync organization aggregates to postgres (organizationSegmentsAgg table), if syncOptions.doSync is true AND syncOptions.withAggs is true.
*/
export async function organizationUpdate(input: IOrganizationProfileSyncInput): Promise<void> {
  // End early if recalculateAffiliations is false, only do syncing if necessary.
  if (!input.recalculateAffiliations) {
    if (input.syncOptions?.doSync) {
      await syncOrganization(input.organization.id)
    }

    return
  }

  const ORGANIZATION_MEMBER_AFFILIATIONS_UPDATED_PER_RUN = 500

  const memberIds = await findMembersInOrganization(
    input.organization.id,
    ORGANIZATION_MEMBER_AFFILIATIONS_UPDATED_PER_RUN,
    input.afterMemberId,
  )

  if (memberIds.length === 0) {
    if (input.syncOptions?.doSync) {
      // sync organization
      await syncOrganization(input.organization.id)
    }
    return
  }

  for (const memberId of memberIds) {
    const memberInput: MemberUpdateInput = {
      member: { id: memberId },
      memberOrganizationIds: [],
      syncToOpensearch: false,
    }
    // Routes through the per-member workflow so concurrent org updates coalesce.
    // ABANDON is needed because continueAsNew closes this execution, and without it
    // Temporal would terminate the child refreshes we just kicked off.
    const handle = await startChild('memberUpdate', {
      workflowId: `${TemporalWorkflowId.MEMBER_UPDATE}/${DEFAULT_TENANT_ID}/${memberId}`,
      workflowIdConflictPolicy: WorkflowIdConflictPolicy.USE_EXISTING,
      parentClosePolicy: ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON,
      taskQueue: 'profiles',
      args: [],
      searchAttributes: { TenantId: [DEFAULT_TENANT_ID] },
    })
    await handle.signal('refreshAffiliations', memberInput)
  }

  await continueAsNew<typeof organizationUpdate>({
    ...input,
    afterMemberId: memberIds[memberIds.length - 1],
  })
}
