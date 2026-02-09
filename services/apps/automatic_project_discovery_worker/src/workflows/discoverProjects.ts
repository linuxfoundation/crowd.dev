import { proxyActivities } from '@temporalio/workflow'

import type * as activities from '../activities'

const activity = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minutes',
})

export async function discoverProjects(): Promise<void> {
  await activity.logDiscoveryRun()
}
