import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../main'
import { discoverProjects } from '../workflows'

export const scheduleProjectsDiscovery = async () => {
  svc.log.info(`Scheduling projects discovery`)

  try {
    await svc.temporal.schedule.create({
      scheduleId: 'automaticProjectsDiscovery',
      spec: {
        cronExpressions: ['55 14 * * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 minute',
      },
      action: {
        type: 'startWorkflow',
        workflowType: discoverProjects,
        taskQueue: 'automatic-projects-discovery',
        args: [{ mode: 'full' as const }],
        workflowExecutionTimeout: '2 hours',
        retry: {
          initialInterval: '15 seconds',
          backoffCoefficient: 2,
          maximumAttempts: 3,
        },
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
