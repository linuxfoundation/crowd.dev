import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../main'
import { discoverProjects } from '../workflows'

const DEFAULT_CRON = '0 2 * * *' // Daily at 2:00 AM

export const scheduleProjectsDiscovery = async () => {
  const cronExpression = process.env.CROWD_AUTOMATIC_PROJECTS_DISCOVERY_CRON || DEFAULT_CRON

  svc.log.info(`Scheduling projects discovery with cron: ${cronExpression}`)

  try {
    await svc.temporal.schedule.create({
      scheduleId: 'automaticProjectsDiscovery',
      spec: {
        cronExpressions: [cronExpression],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 minute',
      },
      action: {
        type: 'startWorkflow',
        workflowType: discoverProjects,
        taskQueue: 'automatic-projects-discovery',
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
