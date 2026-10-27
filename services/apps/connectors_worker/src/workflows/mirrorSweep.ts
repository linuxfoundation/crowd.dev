import {
  ActivityFailure,
  ApplicationFailure,
  TimeoutFailure,
  log,
  proxyActivities,
} from '@temporalio/workflow'

import type * as activities from '../activities/mirrorSweepActivities'
import type { IMirrorSweepInput, IMirrorSweepSummary, MirrorBatchResult } from '../mirrorSweep'
import { mirrorRoom } from '../mirrorSweepLimits'

const activity = proxyActivities<
  Pick<typeof activities, 'countShadowRepos' | 'logMirrorSweepSummary'>
>({
  startToCloseTimeout: '10 minutes',
  retry: {
    maximumAttempts: 3,
    backoffCoefficient: 2,
  },
})

const longActivity = proxyActivities<Pick<typeof activities, 'mirrorBatch'>>({
  startToCloseTimeout: '90 minutes',
  heartbeatTimeout: '5 minutes',
  retry: {
    maximumAttempts: 1,
  },
})

const isPositiveInteger = (value: unknown): boolean => Number.isInteger(value) && Number(value) > 0

function assertValidInput(input: IMirrorSweepInput): void {
  if (!isPositiveInteger(input?.batchSize)) {
    throw ApplicationFailure.nonRetryable(
      `mirrorSweep input batchSize must be a positive integer, got ${JSON.stringify(input?.batchSize)}`,
    )
  }
  if (!isPositiveInteger(input.shadowCap)) {
    throw ApplicationFailure.nonRetryable(
      `mirrorSweep input shadowCap must be a positive integer, got ${JSON.stringify(input.shadowCap)}`,
    )
  }
  if (input.batchSize > input.shadowCap) {
    throw ApplicationFailure.nonRetryable(
      `mirrorSweep input batchSize ${input.batchSize} must not exceed shadowCap ${input.shadowCap}`,
    )
  }
}

function lastReportedProgress(err: unknown): Partial<MirrorBatchResult> {
  if (err instanceof ActivityFailure && err.cause instanceof TimeoutFailure) {
    return (err.cause.lastHeartbeatDetails as Partial<MirrorBatchResult> | undefined) ?? {}
  }
  return {}
}

export async function mirrorSweep(input: IMirrorSweepInput): Promise<IMirrorSweepSummary> {
  assertValidInput(input)
  const startedAt = Date.now()
  const inShadow = await activity.countShadowRepos()
  const summary: IMirrorSweepSummary = {
    batchSize: input.batchSize,
    shadowCap: input.shadowCap,
    inShadow,
    candidates: 0,
    attempted: 0,
    mirrored: 0,
    skipped: 0,
    unitsInserted: 0,
    failed: false,
    durationMs: 0,
  }

  const room = mirrorRoom(input, inShadow)
  if (room > 0) {
    try {
      Object.assign(summary, await longActivity.mirrorBatch(room))
    } catch (err) {
      log.error('mirror sweep batch failed', { err })
      Object.assign(summary, lastReportedProgress(err), { failed: true })
    }
  }

  summary.durationMs = Date.now() - startedAt
  await activity.logMirrorSweepSummary(summary)
  return summary
}
