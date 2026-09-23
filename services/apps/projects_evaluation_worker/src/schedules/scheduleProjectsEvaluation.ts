import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { DISCOVERY_NEW_PROJECTS_LIMIT } from '@crowd/common'

import { svc } from '../main'
import { IEvaluateProjectsInput, evaluateProjects } from '../workflows'

// Ordered list of sources; earlier = higher priority; unlisted sources rank last.
const SOURCE_PRIORITY = ['manual', 'insights-discussions', 'lf-criticality-score']

// Cap high enough that the evaluation queue never carries a residue between runs:
// discovery can accept up to DISCOVERY_NEW_PROJECTS_LIMIT per source (per-source cap),
// so evaluation must be able to drain all sources' worth in one run. It's a ceiling,
// not a target — actual daily volume is well below it.
const EVALUATION_LIMIT = DISCOVERY_NEW_PROJECTS_LIMIT * SOURCE_PRIORITY.length

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
    workflowExecutionTimeout: '3 hours',
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
