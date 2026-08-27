import { log, proxyActivities } from '@temporalio/workflow'

import type * as activities from '../activities/dispatcherActivities'

const activity = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 3, backoffCoefficient: 2 },
})

const CLAIM_LIMIT = 100

export async function dispatcher(): Promise<void> {
  await activity.touchHeartbeat()

  const units = await activity.claimDue(CLAIM_LIMIT)

  const { admitted, deferred } = await activity.admitByBudget(units)

  for (const unit of admitted) {
    try {
      await activity.startRun(unit)
      await activity.reschedule(unit.id, unit.platform, unit.syncName)
    } catch (err) {
      log.error('failed to dispatch sync unit', { unitId: unit.id, err })
    }
  }

  for (const unit of deferred) {
    await activity.deferUnit(unit.id)
  }
}
