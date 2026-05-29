import { proxyActivities } from '@temporalio/workflow'

import type * as activities from './activities'

const { sayHiNpm } = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
})

export async function npmHello(): Promise<void> {
  await sayHiNpm()
}
