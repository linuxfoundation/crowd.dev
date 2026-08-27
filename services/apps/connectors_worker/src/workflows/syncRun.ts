import { proxyActivities } from '@temporalio/workflow'

import type * as activities from '../activities/syncRunActivities'

const activity = proxyActivities<typeof activities>({
  startToCloseTimeout: '30 minutes',
  heartbeatTimeout: '1 minute',
  retry: { maximumAttempts: 1 },
})

export async function syncRun(unitId: string): Promise<void> {
  await activity.executeSync(unitId)
}
