import {
  ApplicationFailure,
  ChildWorkflowCancellationType,
  ParentClosePolicy,
  executeChild,
  proxyActivities,
} from '@temporalio/workflow'

import { IEnrichableMember, MemberEnrichmentSource } from '@crowd/types'

import * as activities from '../activities'
import { IEnrichmentSourceInput } from '../types'
import { sourceHasDifferentDataComparedToCache } from '../utils/common'

import { processMemberSources } from './processMemberSources'

const {
  getEnrichmentData,
  findMemberEnrichmentCache,
  insertMemberEnrichmentCache,
  touchMemberEnrichmentCacheUpdatedAt,
  updateMemberEnrichmentCache,
  isCacheObsolete,
  getEnrichmentInput,
  hasRemainingCredits,
  getMemberById,
} = proxyActivities<typeof activities>({
  startToCloseTimeout: '7 minutes',
  retry: {
    maximumAttempts: 4,
    initialInterval: '5 minutes',
    backoffCoefficient: 2,
    maximumInterval: '20 minutes',
  },
})

export async function enrichMember(
  input: IEnrichableMember,
  sources: MemberEnrichmentSource[],
): Promise<void> {
  const member = await getMemberById(input.id)

  // skip enrichment if member no longer exists
  if (!member) return

  let changeInEnrichmentSourceData = false

  for (const source of sources) {
    // find if there's already saved enrichment data in source
    const caches = await findMemberEnrichmentCache([source], input.id)
    const cache = caches.find((c) => c.source === source)

    // cache is obsolete when it's not found or cache.updatedAt is older than cacheObsoleteAfterSeconds
    if (await isCacheObsolete(source, cache)) {
      const enrichmentInput: IEnrichmentSourceInput = await getEnrichmentInput(input)

      if (!(await hasRemainingCredits(source))) {
        // no credits remaining, throw error to fail the workflow
        throw ApplicationFailure.create({
          message: `No credits remaining for source ${source}`,
          type: 'MEMBER_ENRICHMENT_NO_CREDITS',
          nonRetryable: true,
        })
      }

      const data = await getEnrichmentData(source, enrichmentInput)

      if (!cache) {
        await insertMemberEnrichmentCache(source, input.id, data)
        if (data) {
          changeInEnrichmentSourceData = true
        }
      } else if (data === null && cache.data !== null) {
        await touchMemberEnrichmentCacheUpdatedAt(source, input.id)
      } else if (sourceHasDifferentDataComparedToCache(cache, data)) {
        await updateMemberEnrichmentCache(source, input.id, data)
        changeInEnrichmentSourceData = true
      } else {
        // data is same as cache, only update cache.updatedAt
        await touchMemberEnrichmentCacheUpdatedAt(source, input.id)
      }
    }
  }

  if (changeInEnrichmentSourceData && input.activityCount > 100) {
    // Member enrichment data has been updated, use squasher again!
    await executeChild(processMemberSources, {
      workflowId: 'member-enrichment/' + input.id + '/processMemberSources',
      cancellationType: ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
      parentClosePolicy: ParentClosePolicy.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
      workflowExecutionTimeout: '15 minutes',
      retry: {
        backoffCoefficient: 2,
        maximumAttempts: 5,
        initialInterval: 2 * 1000,
        maximumInterval: 30 * 1000,
      },
      args: [
        {
          memberId: input.id,
          sources,
        },
      ],
    })
  }
}
