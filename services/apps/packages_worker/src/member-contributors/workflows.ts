import { proxyActivities } from '@temporalio/workflow'

import type * as memberContributorActivities from './activities'
import type { GovernanceSyncCounts } from './governance/syncGovernanceContributors'

const { syncGovernanceContributors } = proxyActivities<typeof memberContributorActivities>({
  startToCloseTimeout: '1 hour',
  heartbeatTimeout: '5 minutes',
  retry: {
    initialInterval: '30 seconds',
    backoffCoefficient: 2,
    maximumAttempts: 3,
  },
})

export async function syncGovernanceFileContributors(): Promise<GovernanceSyncCounts> {
  return syncGovernanceContributors()
}
