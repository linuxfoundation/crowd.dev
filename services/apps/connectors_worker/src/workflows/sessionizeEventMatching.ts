import { ActivityFailure, log, proxyActivities } from '@temporalio/workflow'

import type * as activities from '../activities/sessionizeEventMatchingActivities'

const matching = proxyActivities<typeof activities>({
  startToCloseTimeout: '5 minutes',
  retry: {
    initialInterval: '30 seconds',
    backoffCoefficient: 2,
    maximumAttempts: 3,
    nonRetryableErrorTypes: ['ProviderAuthError', 'ProviderContractError'],
  },
})

const reporting = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 3 },
})

function failureMessage(err: unknown): string {
  const failure = err instanceof ActivityFailure && err.cause ? err.cause : err
  return failure instanceof Error ? failure.message : String(failure)
}

async function runAndReport(): Promise<void> {
  const result = await matching.runSessionizeEventMatching()
  if ('skipped' in result) {
    log.info('sessionize event matching skipped', { ...result })
    return
  }
  log.info('sessionize event matching run complete', { ...result.summary })
  if (result.heldReport) {
    await reporting.postSessionizeHeldReport(result.heldReport)
  }
}

export async function sessionizeEventMatching(): Promise<void> {
  try {
    await runAndReport()
  } catch (err) {
    try {
      await reporting.reportSessionizeEventMatchingFailure(failureMessage(err))
    } catch (reportErr) {
      log.error('sessionize event matching: the failure report failed', {
        error: failureMessage(reportErr),
      })
    }
    throw err
  }
}
