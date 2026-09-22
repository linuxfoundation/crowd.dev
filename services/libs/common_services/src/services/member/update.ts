import { DEFAULT_TENANT_ID } from '@crowd/common'
import { Client as TemporalClient, WorkflowIdConflictPolicy } from '@crowd/temporal'
import { TemporalWorkflowId } from '@crowd/types'

interface MemberUpdateOptions {
  memberOrganizationIds?: string[]
  syncToOpensearch?: boolean
}

/**
 * Sends a refreshAffiliations signal to the per-member memberUpdate workflow,
 * starting it if one isn't already running. All callers should go through here
 * so concurrent refreshes coalesce on a single workflow slot per member.
 */
export async function signalMemberUpdate(
  temporal: TemporalClient,
  memberId: string,
  options: MemberUpdateOptions = {},
): Promise<void> {
  await temporal.workflow.signalWithStart('memberUpdate', {
    taskQueue: 'profiles',
    workflowId: `${TemporalWorkflowId.MEMBER_UPDATE}/${memberId}`,
    workflowIdConflictPolicy: WorkflowIdConflictPolicy.USE_EXISTING,
    signal: 'refreshAffiliations',
    signalArgs: [
      {
        member: { id: memberId },
        memberOrganizationIds: options.memberOrganizationIds ?? [],
        syncToOpensearch: options.syncToOpensearch ?? false,
      },
    ],
    retry: { maximumAttempts: 10 },
    args: [],
    searchAttributes: { TenantId: [DEFAULT_TENANT_ID] },
  })
}
