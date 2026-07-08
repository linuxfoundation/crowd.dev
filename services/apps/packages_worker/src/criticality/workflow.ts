import { proxyActivities } from '@temporalio/workflow'

import type * as critActivities from './activities'

const { rankPackages } = proxyActivities<typeof critActivities>({
  startToCloseTimeout: '30 minutes',
  retry: { maximumAttempts: 2 },
})

export async function rankPackagesWorkflow(): Promise<void> {
  await rankPackages()
}
