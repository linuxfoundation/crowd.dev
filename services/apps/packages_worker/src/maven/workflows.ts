import { proxyActivities } from '@temporalio/workflow'

import type * as activities from './activities'

const { processMavenCriticalBatch } = proxyActivities<typeof activities>({
  startToCloseTimeout: '15 minutes',
})

const { processMavenNonCriticalBatch } = proxyActivities<typeof activities>({
  startToCloseTimeout: '5 minutes',
})

export async function mavenCriticalWorkflow(): Promise<void> {
  await processMavenCriticalBatch()
}

export async function mavenNonCriticalWorkflow(): Promise<void> {
  await processMavenNonCriticalBatch()
}
