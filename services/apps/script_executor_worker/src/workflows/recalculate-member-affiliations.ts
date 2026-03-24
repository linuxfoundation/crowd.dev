import { continueAsNew, proxyActivities } from '@temporalio/workflow'

import * as activities from '../activities'
import { IScriptBatchTestArgs } from '../types'
import { chunkArray } from '../utils/common'

const { getMembersForAffiliationRecalc, calculateMemberAffiliations } = proxyActivities<
  typeof activities
>({
  startToCloseTimeout: '30 minutes',
})

export async function recalculateMemberAffiliations(args: IScriptBatchTestArgs): Promise<void> {
  const MEMBERS_PER_RUN = args.batchSize ?? 200

  const memberIds = await getMembersForAffiliationRecalc(MEMBERS_PER_RUN)

  if (memberIds?.length === 0) {
    console.log('No more members to recalculate affiliations!')
    return
  }

  for (const chunk of chunkArray(memberIds, 10)) {
    await Promise.all(chunk.map((memberId) => calculateMemberAffiliations(memberId)))
  }

  if (args.testRun) {
    console.log('Test run completed - stopping after first batch!')
    return
  }

  await continueAsNew<typeof recalculateMemberAffiliations>(args)
}
