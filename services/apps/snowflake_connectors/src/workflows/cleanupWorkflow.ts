import { proxyActivities } from '@temporalio/workflow'

import type * as activities from '../activities/cleanupActivity'

const { executeCleanup } = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 hour',
  retry: {
    initialInterval: '2s',
    backoffCoefficient: 2,
    maximumInterval: '60s',
    maximumAttempts: 3,
  },
})

export async function snowflakeS3CleanupScheduler(): Promise<void> {
  await executeCleanup()
}
