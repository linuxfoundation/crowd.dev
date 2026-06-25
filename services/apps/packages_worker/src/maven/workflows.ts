import { continueAsNew, proxyActivities } from '@temporalio/workflow'

import type * as activities from './activities'

const acts = proxyActivities<typeof activities>({
  startToCloseTimeout: '15 minutes',
  retry: {
    initialInterval: '30 seconds',
    backoffCoefficient: 2,
    maximumAttempts: 5,
  },
})

const BATCHES_PER_RUN = 5

export async function ingestMavenPackages(): Promise<void> {
  for (let i = 0; i < BATCHES_PER_RUN; i++) {
    const result = await acts.processMavenCriticalBatch()
    if (result.processed + result.skipped + result.error + result.unchanged === 0) {
      return
    }
  }

  await continueAsNew<typeof ingestMavenPackages>()
}
