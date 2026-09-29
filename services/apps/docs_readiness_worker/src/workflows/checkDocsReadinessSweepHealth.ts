import { log, patched, proxyActivities, rootCause } from '@temporalio/workflow'

import * as activities from '../activities'

const { checkIncrementalSweepHealth } = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 3 },
})

const { closeStrandedRuns } = proxyActivities<typeof activities>({
  startToCloseTimeout: '5 minutes',
  retry: { maximumAttempts: 2 },
})

export async function checkDocsReadinessSweepHealth(): Promise<void> {
  // Keeps a health check that was already open before the deploy replayable.
  if (patched('close-stranded-runs')) {
    try {
      await closeStrandedRuns()
    } catch (err) {
      log.warn('closing stranded docs readiness runs failed', {
        error: rootCause(err) ?? String(err),
      })
    }
  }

  await checkIncrementalSweepHealth()
}
