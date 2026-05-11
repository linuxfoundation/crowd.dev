import { WorkflowIdConflictPolicy } from '@temporalio/client'

import { DEFAULT_TENANT_ID } from '@crowd/common'
import { refreshMemberOrganizationAffiliations } from '@crowd/data-access-layer/src/member-organization-affiliation'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { SearchSyncApiClient } from '@crowd/opensearch'
import { TemporalWorkflowId } from '@crowd/types'

import { svc } from '../../main'

export async function updateMemberAffiliations(memberId: string): Promise<void> {
  const qx = pgpQx(svc.postgres.writer.connection())
  await refreshMemberOrganizationAffiliations(qx, memberId)
}

export async function triggerMemberAffiliationsRefresh(
  memberId: string,
  memberOrganizationIds: string[] = [],
  syncToOpensearch = false,
): Promise<void> {
  await svc.temporal.workflow.signalWithStart('memberUpdate', {
    taskQueue: 'profiles',
    workflowId: `${TemporalWorkflowId.MEMBER_UPDATE}/${memberId}`,
    workflowIdConflictPolicy: WorkflowIdConflictPolicy.USE_EXISTING,
    signal: 'refreshAffiliations',
    signalArgs: [{ member: { id: memberId }, memberOrganizationIds, syncToOpensearch }],
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
