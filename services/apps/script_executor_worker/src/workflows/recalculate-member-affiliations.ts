import { continueAsNew, proxyActivities } from '@temporalio/workflow'

import * as activities from '../activities'
import { IRecalculateMemberAffiliationsArgs } from '../types'
import { chunkArray } from '../utils/common'

const { getMembersForAffiliationRecalc, triggerMemberAffiliationsRefresh } = proxyActivities<
  typeof activities
>({
  startToCloseTimeout: '30 minutes',
})

export async function recalculateMemberAffiliations(
  args: IRecalculateMemberAffiliationsArgs,
): Promise<void> {
  const MEMBERS_PER_RUN = args.batchSize ?? 200

  const memberIds = args.memberIds
    ? args.memberIds.slice(0, MEMBERS_PER_RUN)
    : await getMembersForAffiliationRecalc(MEMBERS_PER_RUN)

  if (memberIds.length === 0) {
    console.log('No more members to recalculate affiliations!')
    return
  }

  for (const chunk of chunkArray(memberIds, 10)) {
    await Promise.all(chunk.map((memberId) => triggerMemberAffiliationsRefresh(memberId)))
  }

  if (args.testRun) {
    console.log('Test run completed - stopping after first batch!')
    return
  }

  if (args.memberIds) {
    const remaining = args.memberIds.slice(MEMBERS_PER_RUN)
    if (remaining.length === 0) {
      return
    }
    args = { ...args, memberIds: remaining }
  }

  await continueAsNew<typeof recalculateMemberAffiliations>(args)
}
