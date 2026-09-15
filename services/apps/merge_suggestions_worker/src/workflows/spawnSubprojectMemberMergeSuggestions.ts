import {
  ChildWorkflowCancellationType,
  ParentClosePolicy,
  WorkflowIdReusePolicy,
  executeChild,
  proxyActivities,
} from '@temporalio/workflow'

import type * as activities from '../activities'

import { generateSubprojectMemberMergeSuggestions } from './generateSubprojectMemberMergeSuggestions'

const { fetchRecentlyOnboardedSubprojects, fetchCachedSubprojects } = proxyActivities<
  typeof activities
>({
  startToCloseTimeout: '2 minutes',
})

export async function spawnSubprojectMemberMergeSuggestions(): Promise<void> {
  const recentlyOnboarded = await fetchRecentlyOnboardedSubprojects()
  if (recentlyOnboarded.length === 0) {
    return
  }

  const cached = new Set(await fetchCachedSubprojects(recentlyOnboarded))
  const toProcess = recentlyOnboarded.filter((subprojectId) => !cached.has(subprojectId))

  if (toProcess.length === 0) {
    return
  }

  await Promise.all(
    toProcess.map((subprojectId) =>
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
