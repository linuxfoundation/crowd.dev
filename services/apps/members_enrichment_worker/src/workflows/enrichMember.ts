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
import { SOURCES_READING_OTHER_CACHES } from '../utils/config'
import { processMemberSources } from './processMemberSources'

const {
  hasNormalizedDataChanged,
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

  // Fetch caches for all sources upfront to avoid redundant DB round-trips.
  const caches = await findMemberEnrichmentCache(sources, input.id)

  const enrichFromSource = async (source: MemberEnrichmentSource): Promise<boolean> => {
    const cache = caches.find((c) => c.source === source)

    // cache is obsolete when it's not found or cache.updatedAt is older than cacheObsoleteAfterSeconds
    if (!(await isCacheObsolete(source, cache))) return false

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
      return !!data
    }

    if (data === null || !sourceHasDifferentDataComparedToCache(cache, data)) {
      await touchMemberEnrichmentCacheUpdatedAt(source, input.id)
      return false
    }

    await updateMemberEnrichmentCache(source, input.id, data)
    return hasNormalizedDataChanged(source, cache.data, data)
  }

  // Sources reading other sources' caches must start only after those caches are written.
  // The workflow fails fast, but source activities already in flight may still update cache.
  const independentSources = sources.filter((source) => !SOURCES_READING_OTHER_CACHES[source])
  const dependentSources = sources.filter((source) => SOURCES_READING_OTHER_CACHES[source])

  const sourceResults = [
    ...(await Promise.all(independentSources.map(enrichFromSource))),
    ...(await Promise.all(dependentSources.map(enrichFromSource))),
  ]

  const changeInEnrichmentSourceData = sourceResults.some(Boolean)

  if (changeInEnrichmentSourceData) {
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
          activityCount: input.activityCount ?? 0,
          sources,
        },
      ],
    })
  }
}
