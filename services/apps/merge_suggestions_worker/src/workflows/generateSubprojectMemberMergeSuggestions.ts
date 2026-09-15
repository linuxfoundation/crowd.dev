import { proxyActivities } from '@temporalio/workflow'

import type * as activities from '../activities'
import { scoreSubprojectMemberMergePairs } from '../subprojectMemberSimilarity'
import { IProcessGenerateSubprojectMemberMergeSuggestionsArgs } from '../types'

const { fetchSubprojectMemberMergePairs } = proxyActivities<typeof activities>({
  startToCloseTimeout: '10 minutes',
})

const { addMemberToMerge, markSubprojectDone } = proxyActivities<typeof activities>({
  startToCloseTimeout: '2 minutes',
})

export async function generateSubprojectMemberMergeSuggestions(
  args: IProcessGenerateSubprojectMemberMergeSuggestionsArgs,
): Promise<void> {
  const pairs = await fetchSubprojectMemberMergePairs(args.subprojectId)
  const suggestions = scoreSubprojectMemberMergePairs(pairs)

  if (suggestions.length > 0) {
    await addMemberToMerge(suggestions)
  }

  // mark even with zero suggestions so we don't retry daily for the whole window
  await markSubprojectDone(args.subprojectId)
}
