import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../main'
import { dispatcher } from '../workflows/dispatcher'

export async function scheduleDispatcher(): Promise<void> {
  try {
    await svc.temporal.schedule.create({
      scheduleId: 'connectors-dispatcher',
      spec: {
        intervals: [{ every: '30s' }],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 minute',
      },
      action: {
        type: 'startWorkflow',
        workflowType: dispatcher,
        taskQueue: 'connectors',
        args: [],
        workflowExecutionTimeout: '5 minutes',
      },
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info('Dispatcher schedule already registered in Temporal.')
    } else {
      throw new Error(err)
    }
  }
}
