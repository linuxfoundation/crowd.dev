import { continueAsNew, proxyActivities } from '@temporalio/workflow'

import type * as activities from './activities'

const acts = proxyActivities<typeof activities>({
  startToCloseTimeout: '15 minutes',
  heartbeatTimeout: '2 minutes',
  retry: {
    initialInterval: '30 seconds',
    backoffCoefficient: 2,
    maximumAttempts: 5,
  },
})

const BATCH = 100
const ROUNDS_PER_RUN = 200

const START_CURSOR = { criticalAfter: '', after: '' }

export async function enrichGoVersions(cursor = START_CURSOR): Promise<void> {
  for (let r = 0; r < ROUNDS_PER_RUN; r++) {
    const next = await acts.enrichGoVersionsBatch(cursor, BATCH)
    if (next === null) return
    cursor = next
  }
  await continueAsNew<typeof enrichGoVersions>(cursor)
}

export async function enrichGoStatus(cursor = START_CURSOR): Promise<void> {
  for (let r = 0; r < ROUNDS_PER_RUN; r++) {
    const next = await acts.enrichGoStatusBatch(cursor, BATCH)
    if (next === null) return
    cursor = next
  }
  await continueAsNew<typeof enrichGoStatus>(cursor)
}
