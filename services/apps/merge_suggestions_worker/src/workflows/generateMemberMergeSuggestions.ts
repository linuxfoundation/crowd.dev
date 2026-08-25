import { continueAsNew, proxyActivities } from '@temporalio/workflow'

import { IMemberBaseForMergeSuggestions, IMemberMergeSuggestion } from '@crowd/types'

import * as activities from '../activities/memberMergeSuggestions'
import { IProcessGenerateMemberMergeSuggestionsArgs } from '../types'
import { chunkArray } from '../utils'

const activity = proxyActivities<typeof activities>({ startToCloseTimeout: '1 minute' })

export async function generateMemberMergeSuggestions(
  args: IProcessGenerateMemberMergeSuggestionsArgs,
): Promise<void> {
  const PAGE_SIZE = 50
  const PARALLEL_SUGGESTION_PROCESSING = 10
  const SIMILARITY_CONFIDENCE_SCORE_THRESHOLD = 0.75

  let lastUuid: string = args.lastUuid || null

  // get the latest generation time of tenant's member suggestions, we'll only get members created after that for new suggestions
  const lastGeneratedAt = await activity.findTenantsLatestMemberSuggestionGeneratedAt(args.tenantId)

  const result: IMemberBaseForMergeSuggestions[] = await activity.getMembers(
    args.tenantId,
    PAGE_SIZE,
    lastUuid,
    lastGeneratedAt,
  )

  if (result.length === 0) {
    await activity.updateMemberMergeSuggestionsLastGeneratedAt(args.tenantId)
    return
  }

  lastUuid = result.length > 0 ? result[result.length - 1]?.id : null

  const allMergeSuggestions: IMemberMergeSuggestion[] = []

  const promiseChunks = chunkArray(result, PARALLEL_SUGGESTION_PROCESSING)

  for (const chunk of promiseChunks) {
    const mergeSuggestionsPromises: Promise<IMemberMergeSuggestion[]>[] = chunk.map((member) =>
      activity.getMemberMergeSuggestions(args.tenantId, member),
    )

    const mergeSuggestionsResults: IMemberMergeSuggestion[][] =
      await Promise.all(mergeSuggestionsPromises)
    allMergeSuggestions.push(...mergeSuggestionsResults.flat())
  }

  if (allMergeSuggestions.length > 0) {
    // Writes raw and UI together so a rescore below the threshold cannot leave a stale UI row.
    await activity.addMemberToMerge(allMergeSuggestions, SIMILARITY_CONFIDENCE_SCORE_THRESHOLD)
  }

  await continueAsNew<typeof generateMemberMergeSuggestions>({ tenantId: args.tenantId, lastUuid })
}
