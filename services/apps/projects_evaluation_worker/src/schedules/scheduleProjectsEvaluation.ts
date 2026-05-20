import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../main'
import { IEvaluateProjectsInput, evaluateProjects } from '../workflows'

// Priority configuration for the evaluation queue.
// - evaluateLimit: maximum number of projects in 'evaluate' state at any time.
// - sourcePriority: ordered list of sources; earlier = higher priority; unlisted sources rank last.
const EVALUATION_ARGS: IEvaluateProjectsInput = {
  batchSize: 50,
  priorityConfig: {
    evaluateLimit: 50,
    sourcePriority: ['insights-discussions'],
  },
}

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
        args: [EVALUATION_ARGS],
        // 50 projects × ~3min each = ~2.5h worst case; set ceiling with margin.
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
