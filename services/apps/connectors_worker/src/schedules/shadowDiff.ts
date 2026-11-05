import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../main'
import { shadowDiff } from '../workflows/shadowDiff'

const SCHEDULE_ID = 'connectors-shadow-diff'

const action = {
  type: 'startWorkflow',
  workflowType: shadowDiff,
  taskQueue: 'connectors',
  args: [],
  workflowExecutionTimeout: '4 hours',
} as const

export async function scheduleShadowDiff(): Promise<void> {
  try {
    await svc.temporal.schedule.create({
      scheduleId: SCHEDULE_ID,
      spec: {
        calendars: [{ hour: { start: 0, end: 0 } }],
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
      svc.log.info('Shadow diff schedule already registered in Temporal, action reconciled.')
    } else {
      throw new Error(err)
    }
  }
}
