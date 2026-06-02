import { proxyActivities } from '@temporalio/workflow'

import type * as activities from '../activities/exportActivity'

const { executeExport } = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 hour',
  retry: {
    initialInterval: '2s',
    backoffCoefficient: 2,
    maximumInterval: '60s',
    maximumAttempts: 3,
  },
})

export async function pccS3ExportScheduler(): Promise<void> {
  await executeExport()
}
