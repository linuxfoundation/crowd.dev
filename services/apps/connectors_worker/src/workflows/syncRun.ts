import { proxyActivities } from '@temporalio/workflow'

import type * as activities from '../activities/syncRunActivities'
import { RUN_START_TO_CLOSE_TIMEOUT_MS } from '../runLimits'

const activity = proxyActivities<typeof activities>({
  startToCloseTimeout: RUN_START_TO_CLOSE_TIMEOUT_MS,
  heartbeatTimeout: '1 minute',
  retry: { maximumAttempts: 1 },
})

export async function syncRun(unitId: string): Promise<void> {
  await activity.executeSync(unitId)
}
