import { proxyActivities } from '@temporalio/workflow'

import type * as packageRepoActivities from './activities'

const { sweepPackageRepoConfidenceScores } = proxyActivities<typeof packageRepoActivities>({
  // A Temporal timeout does not cancel the in-flight CALL, which keeps the sweep's session
  // advisory lock, so a second attempt could only fail. The daily schedule is the retry.
  startToCloseTimeout: '6 hours',
  retry: { maximumAttempts: 1 },
})

const { reportTiedPackageRepos } = proxyActivities<typeof packageRepoActivities>({
  startToCloseTimeout: '30 minutes',
  retry: { maximumAttempts: 1 },
})

export async function sweepPackageRepoConfidence(): Promise<void> {
  await sweepPackageRepoConfidenceScores()
  await reportTiedPackageRepos()
}
