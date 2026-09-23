import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { DISCOVERY_NEW_PROJECTS_LIMIT } from '@crowd/common'

import { svc } from '../main'
import { IEvaluateProjectsInput, evaluateProjects } from '../workflows'

// Ordered list of sources; earlier = higher priority; unlisted sources rank last.
const SOURCE_PRIORITY = ['manual', 'insights-discussions', 'lf-criticality-score']

// Ceiling, not a target: scheduled (incremental) discovery accepts up to
// DISCOVERY_NEW_PROJECTS_LIMIT per source per run, so evaluation must drain that much.
const EVALUATION_LIMIT = DISCOVERY_NEW_PROJECTS_LIMIT * SOURCE_PRIORITY.length

// Worst case per project: evaluateActivities allows 2 attempts × 3 min startToCloseTimeout.
const WORST_CASE_MINUTES_PER_PROJECT = 6
const WORKFLOW_EXECUTION_TIMEOUT = `${EVALUATION_LIMIT * WORST_CASE_MINUTES_PER_PROJECT} minutes`

function scheduleAction() {
  const args: IEvaluateProjectsInput = {
    batchSize: EVALUATION_LIMIT,
    priorityConfig: {
      evaluateLimit: EVALUATION_LIMIT,
      sourcePriority: SOURCE_PRIORITY,
    },
  }

  return {
    type: 'startWorkflow' as const,
    workflowType: evaluateProjects,
    taskQueue: 'projects-evaluation',
    args: [args] as [IEvaluateProjectsInput],
    workflowExecutionTimeout: WORKFLOW_EXECUTION_TIMEOUT,
    retry: {
      initialInterval: '30 seconds',
      backoffCoefficient: 2,
      maximumAttempts: 3,
    },
  }
}

export const scheduleProjectsEvaluation = async () => {
  svc.log.info({ evaluationLimit: EVALUATION_LIMIT }, 'Scheduling projects evaluation')

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
      action: scheduleAction(),
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info('Schedule projectsEvaluation already exists, reconciling action.')
      const handle = svc.temporal.schedule.getHandle('projectsEvaluation')
      await handle.update((prev) => ({
        ...prev,
        action: scheduleAction(),
      }))
    } else {
      throw new Error(err)
    }
  }
}
