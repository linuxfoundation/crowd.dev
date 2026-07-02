import { continueAsNew, log, proxyActivities } from '@temporalio/workflow'

import type * as activities from './activities'

const acts = proxyActivities<typeof activities>({
  startToCloseTimeout: '30 minutes',
  // Lets a stale attempt (see processBatch.ts heartbeat) detect it's been superseded by a retry
  // and stop, instead of both running concurrently against the same repos.
  heartbeatTimeout: '2 minutes',
  retry: {
    initialInterval: '30 seconds',
    backoffCoefficient: 2,
    maximumAttempts: 5,
  },
})

export async function ingestSecurityContacts(): Promise<void> {
  const result = await acts.processSecurityContactsBatch()
  if (result.processed === 0) {
    log.info('Security contacts ingestion complete — no more work, exiting.')
    return
  }
  await continueAsNew<typeof ingestSecurityContacts>()
}
