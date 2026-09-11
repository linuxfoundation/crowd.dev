import { log, proxyActivities } from '@temporalio/workflow'

import type * as activities from '../activities/dispatcherActivities'

const activity = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 3, backoffCoefficient: 2 },
})

const CLAIM_LIMIT = 100

export async function dispatcher(): Promise<void> {
  const startedAt = Date.now()

  await activity.touchHeartbeat()

  const units = await activity.claimDue(CLAIM_LIMIT)

  const { admitted, deferred } = await activity.admitByBudget(units)

  let started = 0
  let alreadyRunning = 0
  let failed = 0
  for (const unit of admitted) {
    try {
      const result = await activity.startRun(unit)
      if (result === 'started') {
        started += 1
      } else {
        alreadyRunning += 1
      }
    } catch (err) {
      failed += 1
      log.error('failed to dispatch sync unit', { unitId: unit.id, err })
    }
  }

  for (const unit of deferred) {
    await activity.deferUnit(unit.id)
  }

  await activity.logDispatchSummary({
    claimed: units.length,
    admitted: admitted.length,
    deferred: deferred.length,
    started,
    alreadyRunning,
    failed,
    durationMs: Date.now() - startedAt,
  })
}
