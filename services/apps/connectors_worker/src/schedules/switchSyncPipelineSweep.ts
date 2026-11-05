import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../main'
import { switchSyncPipelineSweep } from '../workflows/switchSyncPipelineSweep'

export async function scheduleSwitchSyncPipelineSweep(): Promise<void> {
  try {
    await svc.temporal.schedule.create({
      scheduleId: 'connectors-switch-sync-pipeline-sweep',
      spec: {
        calendars: [{ hour: 2, minute: 0 }],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 hour',
      },
      action: {
        type: 'startWorkflow',
        workflowType: switchSyncPipelineSweep,
        taskQueue: 'connectors',
        args: [],
      },
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info('Switch sync pipeline sweep schedule already registered in Temporal.')
    } else {
      throw new Error(err)
    }
  }
}
