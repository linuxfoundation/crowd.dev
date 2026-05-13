import {
  ChildWorkflowCancellationType,
  ParentClosePolicy,
  continueAsNew,
  executeChild,
  proxyActivities,
} from '@temporalio/workflow'

import { IEnrichableMember, MemberEnrichmentSource } from '@crowd/types'

import * as activities from '../activities'
import { chunkArray } from '../utils/common'

import { enrichMember } from './enrichMember'

const { getEnrichableMembers, getMaxConcurrentRequests } = proxyActivities<typeof activities>({
  startToCloseTimeout: '15 minutes',
})

export async function triggerMembersEnrichment(): Promise<void> {
  const QUERY_FOR_ENRICHABLE_MEMBERS_PER_RUN = 500
  const sources = [
    MemberEnrichmentSource.PROGAI,
    MemberEnrichmentSource.CLEARBIT,
    MemberEnrichmentSource.PROGAI_LINKEDIN_SCRAPER,
    MemberEnrichmentSource.CRUSTDATA,
  ]

  const members = await getEnrichableMembers(QUERY_FOR_ENRICHABLE_MEMBERS_PER_RUN, sources)

  if (members.length === 0) {
    return
  }

  const parallelEnrichmentWorkflows = await getMaxConcurrentRequests(
    members,
    sources,
    QUERY_FOR_ENRICHABLE_MEMBERS_PER_RUN,
  )

  const chunks = chunkArray<IEnrichableMember>(members, parallelEnrichmentWorkflows)

  for (const chunk of chunks) {
    await Promise.all(
      chunk.map((member) =>
        executeChild(enrichMember, {
          workflowId: 'member-enrichment/' + member.id,
          cancellationType: ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
          parentClosePolicy: ParentClosePolicy.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
          workflowExecutionTimeout: '60 minutes',
          args: [member, sources],
        }),
      ),
    )
  }

  await continueAsNew<typeof triggerMembersEnrichment>()
}
