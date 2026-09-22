import { continueAsNew, log, proxyActivities } from '@temporalio/workflow'

import type * as activities from './activities'

const acts = proxyActivities<typeof activities>({
  startToCloseTimeout: '30 minutes',
  retry: {
    initialInterval: '30 seconds',
    backoffCoefficient: 2,
    maximumAttempts: 5,
  },
})

export async function ingestNuGetPackages(): Promise<void> {
  const result = await acts.processNuGetBatch()
  if (result.processed + result.skipped + result.error + result.unchanged === 0) {
    log.info('NuGet ingestion complete — no more work, exiting.', { ...result })
    return
  }
  await continueAsNew<typeof ingestNuGetPackages>()
}
