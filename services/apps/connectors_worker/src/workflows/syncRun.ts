import {
  ActivityFailure,
  ApplicationFailure,
  TimeoutFailure,
  proxyActivities,
} from '@temporalio/workflow'

import type * as activities from '../activities/syncRunActivities'
import { RUN_START_TO_CLOSE_TIMEOUT_MS } from '../runLimits'
import { SYNC_RUN_FAILURE_RECORDED } from '../syncRunFailure'

const activity = proxyActivities<typeof activities>({
  startToCloseTimeout: RUN_START_TO_CLOSE_TIMEOUT_MS,
  heartbeatTimeout: '1 minute',
  retry: { maximumAttempts: 1 },
})

const timeoutRecorder = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 5 },
})

function isRecordedRunFailure(cause: Error | undefined): boolean {
  return cause instanceof ApplicationFailure && cause.type === SYNC_RUN_FAILURE_RECORDED
}

export async function syncRun(unitId: string): Promise<void> {
  const attemptStartedAt = new Date().toISOString()
  try {
    await activity.executeSync(unitId)
  } catch (err) {
    if (!(err instanceof ActivityFailure)) {
      throw err
    }
    if (err.cause instanceof TimeoutFailure) {
      await timeoutRecorder.recordSyncRunTimeout(unitId, err.cause.timeoutType, attemptStartedAt)
      return
    }
    if (!isRecordedRunFailure(err.cause)) {
      throw err
    }
  }
}
