import { continueAsNew, log, proxyActivities } from '@temporalio/workflow'

import type * as activities from './activities'

const acts = proxyActivities<typeof activities>({
  startToCloseTimeout: '15 minutes',
  retry: {
    initialInterval: '30 seconds',
    backoffCoefficient: 2,
    maximumAttempts: 5,
  },
})

export async function ingestMavenPackages(): Promise<void> {
  const result = await acts.processMavenCriticalBatch()
  if (result.processed + result.skipped + result.unchanged === 0) {
    log.info('Maven ingestion complete — no more work, exiting.', { ...result })
    return
  }
  await continueAsNew<typeof ingestMavenPackages>()
}
