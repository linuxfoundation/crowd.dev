import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../main'
import { selfHealStarBackfill } from '../workflows'

export const scheduleSelfHealStarBackfill = async () => {
  try {
    await svc.temporal.schedule.create({
      scheduleId: 'selfHealStarBackfill',
      spec: {
        cronExpressions: ['0 9 * * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.BUFFER_ONE,
        catchupWindow: '1 minute',
      },
      action: {
        type: 'startWorkflow',
        workflowType: selfHealStarBackfill,
        taskQueue: 'star-snapshot',
        retry: {
          initialInterval: '15 seconds',
          backoffCoefficient: 2,
          maximumAttempts: 3,
        },
        args: [],
      },
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info('Schedule already registered in Temporal.')
      svc.log.info('Configuration may have changed since. Please make sure they are in sync.')
    } else {
      throw new Error(err)
    }
  }
}
