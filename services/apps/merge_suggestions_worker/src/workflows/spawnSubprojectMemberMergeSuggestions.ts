import {
  ChildWorkflowCancellationType,
  ParentClosePolicy,
  WorkflowIdReusePolicy,
  executeChild,
  proxyActivities,
} from '@temporalio/workflow'

import type * as activities from '../activities/subprojectMemberMergeSuggestions'

import { generateSubprojectMemberMergeSuggestions } from './generateSubprojectMemberMergeSuggestions'

const { fetchRecentlyOnboardedSubprojects } = proxyActivities<typeof activities>({
  startToCloseTimeout: '2 minutes',
})

export async function spawnSubprojectMemberMergeSuggestions(): Promise<void> {
  const subprojectIds = await fetchRecentlyOnboardedSubprojects()

  await Promise.all(
    subprojectIds.map((subprojectId) =>
      executeChild(generateSubprojectMemberMergeSuggestions, {
        workflowId: `generate-subproject-member-merge-suggestions/${subprojectId}`,
        workflowIdReusePolicy: WorkflowIdReusePolicy.ALLOW_DUPLICATE,
        cancellationType: ChildWorkflowCancellationType.ABANDON,
        parentClosePolicy: ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON,
        retry: {
          backoffCoefficient: 2,
          initialInterval: 2 * 1000,
          maximumInterval: 30 * 1000,
        },
        args: [{ subprojectId }],
      }),
    ),
  )
}
