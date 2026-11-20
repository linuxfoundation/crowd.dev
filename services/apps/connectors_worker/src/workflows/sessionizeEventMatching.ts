import { ActivityFailure, log, proxyActivities } from '@temporalio/workflow'

import type * as activities from '../activities/sessionizeEventMatchingActivities'

const activity = proxyActivities<typeof activities>({
  startToCloseTimeout: '10 minutes',
  retry: {
    maximumAttempts: 3,
    nonRetryableErrorTypes: ['ProviderAuthError', 'ProviderContractError'],
  },
})

function failureMessage(err: unknown): string {
  const failure = err instanceof ActivityFailure && err.cause ? err.cause : err
  return failure instanceof Error ? failure.message : String(failure)
}

export async function sessionizeEventMatching(): Promise<void> {
  try {
    const result = await activity.runSessionizeEventMatching()
    log.info('sessionize event matching run complete', { ...result })
  } catch (err) {
    await activity.reportSessionizeEventMatchingFailure(failureMessage(err))
    throw err
  }
}
