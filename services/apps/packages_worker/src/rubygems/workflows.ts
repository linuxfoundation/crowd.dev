import { continueAsNew, log, proxyActivities } from '@temporalio/workflow'

import type * as activities from './activities'

const acts = proxyActivities<typeof activities>({
  startToCloseTimeout: '30 minutes',
  retry: { initialInterval: '30 seconds', backoffCoefficient: 2, maximumAttempts: 5 },
})

export async function ingestRubyGemsPackages(): Promise<void> {
  const result = await acts.processRubyGemsCoreBatch()
  if (result.processed + result.skipped + result.error + result.unchanged === 0) {
    log.info('RubyGems core ingestion complete — no more work, exiting.', { ...result })
    return
  }
  await continueAsNew<typeof ingestRubyGemsPackages>()
}

export async function ingestRubyGemsCriticalDetails(afterId = 0): Promise<void> {
  const result = await acts.processRubyGemsCriticalBatch(afterId)
  if (result.lastId === null) {
    log.info('RubyGems critical ingestion complete — no more work, exiting.', { ...result })
    return
  }
  await continueAsNew<typeof ingestRubyGemsCriticalDetails>(result.lastId)
}

export async function ingestRubyGemsDependents(afterId = 0): Promise<void> {
  const result = await acts.processRubyGemsDependentsBatch(afterId)
  if (result.lastId === null) {
    log.info('RubyGems dependents ingestion complete — no more work, exiting.', { ...result })
    return
  }
  await continueAsNew<typeof ingestRubyGemsDependents>(result.lastId)
}
