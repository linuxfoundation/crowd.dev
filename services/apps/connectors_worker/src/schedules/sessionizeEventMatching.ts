import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../main'
import { sessionizeEventMatching } from '../workflows/sessionizeEventMatching'

export async function scheduleSessionizeEventMatching(): Promise<void> {
  try {
    await svc.temporal.schedule.create({
      scheduleId: 'connectors-sessionize-event-matching',
      spec: {
        calendars: [{ hour: 5, minute: 30 }],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 hour',
      },
      action: {
        type: 'startWorkflow',
        workflowType: sessionizeEventMatching,
        taskQueue: 'connectors',
        args: [],
        workflowExecutionTimeout: '30 minutes',
      },
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info('Sessionize event matching schedule already registered in Temporal.')
    } else {
      throw new Error(err)
    }
  }
}
