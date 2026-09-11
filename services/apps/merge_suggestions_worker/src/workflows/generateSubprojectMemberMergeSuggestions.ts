import { proxyActivities } from '@temporalio/workflow'

import type * as memberMergeActivities from '../activities/memberMergeSuggestions'
import type * as subprojectActivities from '../activities/subprojectMemberMergeSuggestions'
import { scoreSubprojectMemberMergePairs } from '../subprojectMemberSimilarity'
import { IProcessGenerateSubprojectMemberMergeSuggestionsArgs } from '../types'

const { fetchSubprojectMemberMergePairs } = proxyActivities<typeof subprojectActivities>({
  startToCloseTimeout: '10 minutes',
})

const { addMemberToMerge } = proxyActivities<typeof memberMergeActivities>({
  startToCloseTimeout: '2 minutes',
})

export async function generateSubprojectMemberMergeSuggestions(
  args: IProcessGenerateSubprojectMemberMergeSuggestionsArgs,
): Promise<void> {
  const pairs = await fetchSubprojectMemberMergePairs(args.subprojectId)
  const suggestions = scoreSubprojectMemberMergePairs(pairs)

  if (suggestions.length === 0) {
    return
  }

  await addMemberToMerge(suggestions)
}
