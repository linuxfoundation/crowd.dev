import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../main'
import { evaluateProjects } from '../workflows'

export const scheduleProjectsEvaluation = async () => {
  svc.log.info('Scheduling projects evaluation')

  try {
    await svc.temporal.schedule.create({
      scheduleId: 'projectsEvaluation',
      spec: {
        // Run every Monday at 02:00 UTC — gives time after the weekly discovery run.
        cronExpressions: ['0 2 * * 1'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 minute',
      },
      action: {
        type: 'startWorkflow',
        workflowType: evaluateProjects,
        taskQueue: 'projects-evaluation',
        args: [{ batchSize: 100 }],
        // 100 projects × ~3min each = ~5h worst case; set ceiling with margin.
        workflowExecutionTimeout: '6 hours',
        retry: {
          initialInterval: '30 seconds',
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
