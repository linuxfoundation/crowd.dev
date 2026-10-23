import {
  ChildWorkflowCancellationType,
  ParentClosePolicy,
  continueAsNew,
  executeChild,
  proxyActivities,
} from '@temporalio/workflow'

import { IEnrichableOrganization, OrganizationEnrichmentSource } from '@crowd/types'

import * as activities from '../activities'
import { ITriggerOneTimeOrganizationsEnrichmentInput } from '../types'
import { chunkArray } from '../utils/common'
import { enrichOrganization } from '../workflows'

const { getOrganizationsForOneTimeEnrichment } = proxyActivities<typeof activities>({
  startToCloseTimeout: '10 minutes',
})

export async function triggerOneTimeOrganizationsEnrichment(
  input: ITriggerOneTimeOrganizationsEnrichmentInput,
): Promise<void> {
  const batchSize = input.batchSize ?? 500
  const concurrency = input.concurrency ?? 4
  const source = OrganizationEnrichmentSource.LFX_INTERNAL_API

  const batchIds = input.organizationIds.slice(0, batchSize)
  const remainingIds = input.organizationIds.slice(batchSize)

  const organizations = await getOrganizationsForOneTimeEnrichment(batchIds, source)

  for (const chunk of chunkArray<IEnrichableOrganization>(organizations, concurrency)) {
    await Promise.all(
      chunk.map((organization) =>
        executeChild(enrichOrganization, {
          workflowId: 'organization-enrichment/' + organization.id,
          cancellationType: ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
          parentClosePolicy: ParentClosePolicy.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
          workflowExecutionTimeout: '15 minutes',
          retry: {
            backoffCoefficient: 2,
            maximumAttempts: 10,
            initialInterval: '60s',
            maximumInterval: '5 minutes',
          },
          args: [organization, source, { skipEnrichableCheck: true }],
        }),
      ),
    )
  }

  if (input.testRun || remainingIds.length === 0) {
    return
  }

  await continueAsNew<typeof triggerOneTimeOrganizationsEnrichment>({
    ...input,
    organizationIds: remainingIds,
  })
}
