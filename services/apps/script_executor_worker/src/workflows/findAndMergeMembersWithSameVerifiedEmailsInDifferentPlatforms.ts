import { continueAsNew, proxyActivities } from '@temporalio/workflow'

import * as commonActivities from '../activities/common'
import * as activities from '../activities/merge-members-with-similar-identities'
import { IFindAndMergeMembersWithSameVerifiedEmailsInDifferentPlatformsArgs } from '../types'

const activity = proxyActivities<typeof activities>({
  startToCloseTimeout: '3 minute',
  retry: { maximumAttempts: 3 },
})

const common = proxyActivities<typeof commonActivities>({
  startToCloseTimeout: '5 minute',
  retry: { maximumAttempts: 6, backoffCoefficient: 3 },
})

export async function findAndMergeMembersWithSameVerifiedEmailsInDifferentPlatforms(
  args: IFindAndMergeMembersWithSameVerifiedEmailsInDifferentPlatformsArgs,
): Promise<void> {
  const PROCESS_MEMBERS_PER_RUN = 1000

  const mergeableMemberCouples =
    await activity.findMembersWithSameVerifiedEmailsInDifferentPlatforms(
      PROCESS_MEMBERS_PER_RUN,
      args.afterHighMemberId || undefined,
      args.afterLowMemberId || undefined,
    )

  if (mergeableMemberCouples.length === 0) {
    console.log(`Finished processing!`)
    return
  }

  for (const couple of mergeableMemberCouples) {
    const coupleDescription = `${couple.secondaryMemberId} [${couple.secondaryMemberIdentityValue}] into ${couple.primaryMemberId} [${couple.primaryMemberIdentityValue}]`

    if (args.dryRun) {
      console.log(`[dry run] Would merge ${coupleDescription}!`)
      continue
    }

    console.log(`Merging ${coupleDescription}!`)

    await common.mergeMembersIfAllowed(couple.primaryMemberId, couple.secondaryMemberId)
  }

  const lastCouple = mergeableMemberCouples[mergeableMemberCouples.length - 1]
  const [afterLowMemberId, afterHighMemberId] = [
    lastCouple.primaryMemberId,
    lastCouple.secondaryMemberId,
  ].sort()

  await continueAsNew<typeof findAndMergeMembersWithSameVerifiedEmailsInDifferentPlatforms>({
    afterHighMemberId,
    afterLowMemberId,
    dryRun: args.dryRun,
  })
}
