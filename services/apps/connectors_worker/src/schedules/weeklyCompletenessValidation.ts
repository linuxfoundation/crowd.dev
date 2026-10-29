import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../main'
import { weeklyCompletenessValidation } from '../workflows/weeklyCompletenessValidation'

const SCHEDULE_ID = 'connectors-weekly-completeness-validation'

const action = {
  type: 'startWorkflow',
  workflowType: weeklyCompletenessValidation,
  taskQueue: 'connectors',
  args: [],
  workflowExecutionTimeout: '6 days',
} as const

export async function scheduleWeeklyCompletenessValidation(): Promise<void> {
  try {
    await svc.temporal.schedule.create({
      scheduleId: SCHEDULE_ID,
      spec: {
        calendars: [{ dayOfWeek: 'SATURDAY', hour: 6 }],
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
        'Weekly completeness validation schedule already registered in Temporal, action reconciled.',
      )
    } else {
      throw new Error(err)
    }
  }
}
