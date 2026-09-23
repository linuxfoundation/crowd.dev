import { proxyActivities } from '@temporalio/workflow'

import * as activities from '../activities'

const { checkIncrementalSweepHealth } = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 3 },
})

export async function checkDocsReadinessSweepHealth(): Promise<void> {
  await checkIncrementalSweepHealth()
}
