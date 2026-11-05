import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../main'
import { completenessBackfillSweep } from '../workflows/completenessBackfillSweep'

const SCHEDULE_ID = 'connectors-completeness-backfill-sweep'

const action = {
  type: 'startWorkflow',
  workflowType: completenessBackfillSweep,
  taskQueue: 'connectors',
  args: [],
  workflowExecutionTimeout: '3 hours',
} as const

export async function scheduleCompletenessBackfillSweep(): Promise<void> {
  try {
    await svc.temporal.schedule.create({
      scheduleId: SCHEDULE_ID,
      spec: {
        calendars: [{ minute: 20, hour: { start: 0, end: 23 } }],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 hour',
      },
      action,
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      await svc.temporal.schedule.getHandle(SCHEDULE_ID).update((prev) => ({ ...prev, action }))
      svc.log.info(
        'Completeness backfill sweep schedule already registered in Temporal, action reconciled.',
      )
    } else {
      throw new Error(err)
    }
  }
}
