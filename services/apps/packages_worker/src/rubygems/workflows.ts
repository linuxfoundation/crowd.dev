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

export async function ingestRubyGemsCriticalDetails(): Promise<void> {
  const result = await acts.processRubyGemsCriticalBatch()
  if (result.processed + result.skipped + result.error + result.unchanged === 0) {
    log.info('RubyGems critical ingestion complete — no more work, exiting.', { ...result })
    return
  }
  await continueAsNew<typeof ingestRubyGemsCriticalDetails>()
}

export async function ingestRubyGemsDependents(): Promise<void> {
  const result = await acts.processRubyGemsDependentsBatch()
  if (result.processed + result.notFound === 0) {
    log.info('RubyGems dependents ingestion complete — no more progress, exiting.', { ...result })
    return
  }
  await continueAsNew<typeof ingestRubyGemsDependents>()
}
