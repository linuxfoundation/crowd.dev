import { continueAsNew, log, proxyActivities } from '@temporalio/workflow'

import type * as activities from './activities'
import type { IngestSingleResult } from './ingestSingle'

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

// Single-repo, synchronous, on-demand path: short bound (no continueAsNew/heartbeat —
// one repo's extractors, not a whole sweep), so a caller awaiting the result (e.g. the
// akrites API on a cache miss) doesn't hang.
const singleActs = proxyActivities<typeof activities>({
  startToCloseTimeout: '45 seconds',
  retry: {
    initialInterval: '5 seconds',
    maximumAttempts: 2,
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

export async function ingestSecurityContactsForPurlWorkflow(
  purl: string,
): Promise<IngestSingleResult> {
  return singleActs.ingestSecurityContactsForPurlActivity(purl)
}
