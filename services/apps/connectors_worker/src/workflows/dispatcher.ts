import { log, proxyActivities } from '@temporalio/workflow'

import type { IClaimedUnit } from '@crowd/data-access-layer/src/connectors'

import type * as activities from '../activities/dispatcherActivities'
import { chunk } from '../chunk'
import type { StartRunResult } from '../types'

const activity = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 3, backoffCoefficient: 2 },
})

const CLAIM_LIMIT = 300
const START_CONCURRENCY = 25

type StartOutcome = StartRunResult | 'failed'

export async function dispatcher(): Promise<void> {
  const startedAt = Date.now()

  await activity.touchHeartbeat()

  const units = await activity.claimDue(CLAIM_LIMIT)

  const { admitted, deferred } = await activity.admitByBudget(units)

  const outcomes: Record<StartOutcome, number> = { started: 0, alreadyRunning: 0, failed: 0 }
  for (const batch of chunk(admitted, START_CONCURRENCY)) {
    for (const outcome of await Promise.all(batch.map(startUnit))) {
      outcomes[outcome] += 1
    }
  }

  for (const unit of deferred) {
    await activity.deferUnit(unit.id, unit.leaseLapsed)
  }

  await activity.logDispatchSummary({
    claimed: units.length,
    admitted: admitted.length,
    deferred: deferred.length,
    started: outcomes.started,
    alreadyRunning: outcomes.alreadyRunning,
    failed: outcomes.failed,
    durationMs: Date.now() - startedAt,
  })
}

async function startUnit(unit: IClaimedUnit): Promise<StartOutcome> {
  try {
    return await activity.startRun(unit)
  } catch (err) {
    log.error('failed to dispatch sync unit', { unitId: unit.id, err })
    return 'failed'
  }
}
