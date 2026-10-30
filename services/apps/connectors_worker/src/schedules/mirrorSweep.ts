import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../main'
import { mirrorSweep } from '../workflows/mirrorSweep'

export async function scheduleMirrorSweep(): Promise<void> {
  try {
    await svc.temporal.schedule.create({
      scheduleId: 'connectors-mirror-sweep',
      spec: {
        calendars: [{ hour: 3, minute: 15 }],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 hour',
      },
      action: {
        type: 'startWorkflow',
        workflowType: mirrorSweep,
        taskQueue: 'connectors',
        args: [{ batchSize: 2500, shadowCap: 5000 }],
        workflowExecutionTimeout: '2 hours',
      },
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info('Mirror sweep schedule already registered in Temporal.')
    } else {
      throw new Error(err)
    }
  }
}
