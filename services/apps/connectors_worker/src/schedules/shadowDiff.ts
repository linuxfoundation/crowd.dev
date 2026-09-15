import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../main'
import { shadowDiff } from '../workflows/shadowDiff'

export async function scheduleShadowDiff(): Promise<void> {
  try {
    await svc.temporal.schedule.create({
      scheduleId: 'connectors-shadow-diff',
      spec: {
        calendars: [{ hour: { start: 0, end: 0 } }],
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
        // Each run only processes one bounded batch of channels before continuing as new, so
        // workflowRunTimeout only needs to cover a single batch's worst-case activity retries.
        // workflowExecutionTimeout is a safety net for the whole continue-as-new chain in case
        // of an unexpectedly large number of channels.
        workflowRunTimeout: '20 minutes',
        workflowExecutionTimeout: '6 hours',
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
