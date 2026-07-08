import {
  ChildWorkflowCancellationType,
  ParentClosePolicy,
  continueAsNew,
  executeChild,
  proxyActivities,
} from '@temporalio/workflow'

import * as activities from '../../activities'
import { IGetMembersForLFIDEnrichmentArgs } from '../../sources/lfid/types'
import { enrichMemberWithLFAuth0 } from '../lf-auth0/enrichMemberWithLFAuth0'

const { getLFIDEnrichableMembers } = proxyActivities<typeof activities>({
  startToCloseTimeout: '10 seconds',
})

const { refreshToken } = proxyActivities<typeof activities>({
  startToCloseTimeout: '2 minutes',
  retry: {
    initialInterval: '2 seconds',
    backoffCoefficient: 2,
    maximumAttempts: 3,
  },
})

export async function getMembersForLFIDEnrichment(
  args: IGetMembersForLFIDEnrichmentArgs,
): Promise<void> {
  const MEMBER_ENRICHMENT_PER_RUN = 10
  const afterId = args?.afterId || null
  const members = await getLFIDEnrichableMembers(MEMBER_ENRICHMENT_PER_RUN, afterId)

  if (members.length === 0) {
    return
  }

  const token = await refreshToken()

  for (const member of members) {
    await executeChild(enrichMemberWithLFAuth0, {
      workflowId: 'member-enrichment-lfid/' + member.id,
      cancellationType: ChildWorkflowCancellationType.ABANDON,
      parentClosePolicy: ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON,
      workflowExecutionTimeout: '10 minutes',
      retry: {
        backoffCoefficient: 2,
        maximumAttempts: 10,
        initialInterval: 2 * 1000,
        maximumInterval: 30 * 1000,
      },
      args: [token, member],
    })
  }

  await continueAsNew<typeof getMembersForLFIDEnrichment>({
    afterId: members[members.length - 1].id,
  })
}
