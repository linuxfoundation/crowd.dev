import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../main'
import { shadowDiff } from '../workflows/shadowDiff'

export async function scheduleShadowDiff(): Promise<void> {
  try {
    await svc.temporal.schedule.create({
      scheduleId: 'connectors-shadow-diff',
      spec: {
        calendars: [{ hour: { start: 0, end: 1 } }],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 hour',
      },
      action: {
        type: 'startWorkflow',
        workflowType: shadowDiff,
        taskQueue: 'connectors',
        args: [],
        workflowExecutionTimeout: '30 minutes',
      },
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info('Shadow diff schedule already registered in Temporal.')
    } else {
      throw new Error(err)
    }
  }
}
