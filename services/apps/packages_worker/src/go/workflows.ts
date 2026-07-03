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

interface ScanState {
  runStartedAt: string
}

export async function enrichGoVersions(state?: ScanState): Promise<void> {
  const runStartedAt = state?.runStartedAt ?? (await acts.getGoRunStartedAt())
  for (let r = 0; r < ROUNDS_PER_RUN; r++) {
    const count = await acts.enrichGoVersionsBatch(runStartedAt, BATCH)
    if (count === 0) return
  }
  await continueAsNew<typeof enrichGoVersions>({ runStartedAt })
}

export async function enrichGoStatus(state?: ScanState): Promise<void> {
  const runStartedAt = state?.runStartedAt ?? (await acts.getGoRunStartedAt())
  for (let r = 0; r < ROUNDS_PER_RUN; r++) {
    const count = await acts.enrichGoStatusBatch(runStartedAt, BATCH)
    if (count === 0) return
  }
  await continueAsNew<typeof enrichGoStatus>({ runStartedAt })
}
