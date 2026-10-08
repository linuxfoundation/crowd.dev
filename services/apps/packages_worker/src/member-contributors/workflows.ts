import { proxyActivities } from '@temporalio/workflow'

import type * as memberContributorActivities from './activities'
import type {
  GitActivitySyncCounts,
  GitActivitySyncOptions,
} from './git-activity/syncGitActivityContributors'
import type { GovernanceSyncCounts } from './governance/syncGovernanceContributors'

const RETRY = {
  initialInterval: '30 seconds',
  backoffCoefficient: 2,
  maximumAttempts: 3,
}

const { syncGovernanceContributors } = proxyActivities<typeof memberContributorActivities>({
  startToCloseTimeout: '1 hour',
  heartbeatTimeout: '5 minutes',
  retry: RETRY,
})

const { syncGitActivityContributors } = proxyActivities<typeof memberContributorActivities>({
  startToCloseTimeout: '4 hours',
  heartbeatTimeout: '10 minutes',
  retry: RETRY,
})

export async function syncGovernanceFileContributors(): Promise<GovernanceSyncCounts> {
  return syncGovernanceContributors()
}

export async function syncRepoContributorsFromGitActivity(
  options: GitActivitySyncOptions,
): Promise<GitActivitySyncCounts> {
  return syncGitActivityContributors(options)
}
