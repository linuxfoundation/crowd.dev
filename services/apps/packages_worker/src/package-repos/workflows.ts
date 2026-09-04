import { proxyActivities } from '@temporalio/workflow'

import type * as packageRepoActivities from './activities'

const { sweepPackageRepoConfidenceScores } = proxyActivities<typeof packageRepoActivities>({
  startToCloseTimeout: '6 hours',
  retry: { maximumAttempts: 2, initialInterval: '5 minutes', backoffCoefficient: 2 },
})

const { assertNoTiedPackageRepos } = proxyActivities<typeof packageRepoActivities>({
  startToCloseTimeout: '30 minutes',
  retry: { maximumAttempts: 1 },
})

export async function sweepPackageRepoConfidence(): Promise {
  await sweepPackageRepoConfidenceScores()
  await assertNoTiedPackageRepos()
}
