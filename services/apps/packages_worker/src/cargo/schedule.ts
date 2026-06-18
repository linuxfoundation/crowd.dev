import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../service'
import { cargoSyncWorkflow } from '../workflows'

const SCHEDULE_ID = 'cargo-registry-sync'

// Daily at 06:00 UTC — clear of the 03:00–05:00 window other ingest jobs use,
// and well after the crates.io dump publishes (~02:00 UTC).
export async function scheduleCargoSync(): Promise<void> {
  const { temporal } = svc
  if (!temporal) throw new Error('Temporal client not initialized')

  try {
    await temporal.schedule.create({
      scheduleId: SCHEDULE_ID,
      spec: { cronExpressions: ['0 6 * * *'] },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 hour',
      },
      action: {
        type: 'startWorkflow',
        workflowType: cargoSyncWorkflow,
        taskQueue: 'cargo-worker',
        workflowExecutionTimeout: '3 hours',
        retry: {
          initialInterval: '30 seconds',
          backoffCoefficient: 2,
          maximumAttempts: 3,
        },
        args: [],
      },
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info(`Schedule ${SCHEDULE_ID} already registered.`)
    } else {
      throw err
    }
  }
}
