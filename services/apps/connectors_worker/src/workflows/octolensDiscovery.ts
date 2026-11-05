import { log, proxyActivities } from '@temporalio/workflow'

import type * as activities from '../activities/octolensDiscoveryActivities'

// One retry only: a failed run (Octolens down, rate limited) is simply retried by the next hourly
// run, and every request here counts against the shared 500/hour Octolens budget.
const activity = proxyActivities<typeof activities>({
  startToCloseTimeout: '10 minutes',
  retry: {
    maximumAttempts: 2,
    backoffCoefficient: 2,
    nonRetryableErrorTypes: ['ProviderAuthError'],
  },
})

export async function octolensDiscovery(): Promise<void> {
  const result = await activity.runOctolensDiscovery()
  log.info('octolens discovery run complete', { ...result })
}
