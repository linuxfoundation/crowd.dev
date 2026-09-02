import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../main'
import { IEvaluateProjectsInput, evaluateProjects } from '../workflows'

// Priority configuration for the evaluation queue.
// - evaluateLimit: maximum number of projects in 'evaluate' state at any time.
// - sourcePriority: ordered list of sources; earlier = higher priority; unlisted sources rank last.
const EVALUATION_ARGS: IEvaluateProjectsInput = {
  batchSize: 20,
  priorityConfig: {
    evaluateLimit: 20,
    sourcePriority: ['manual', 'insights-discussions'],
  },
}

export const scheduleProjectsEvaluation = async () => {
  svc.log.info('Scheduling projects evaluation')

  try {
    await svc.temporal.schedule.create({
      scheduleId: 'projectsEvaluation',
      spec: {
        cronExpressions: ['0 4 * * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 hour',
      },
      action: {
        type: 'startWorkflow',
        workflowType: evaluateProjects,
        taskQueue: 'projects-evaluation',
        args: [EVALUATION_ARGS],
        workflowExecutionTimeout: '3 hours',
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
