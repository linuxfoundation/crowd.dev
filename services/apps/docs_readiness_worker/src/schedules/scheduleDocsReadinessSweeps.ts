import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../main'
import { MAX_CONCURRENT_WORKERS } from '../scoring/runChecksIsolated'
import { DOCS_READINESS_TASK_QUEUE } from '../types'
import { checkDocsReadinessSweepHealth, runDocsReadinessSweep } from '../workflows'

// Bounds a wedged execution so `overlap: SKIP` cannot block every later run of the schedule.
// The incremental sweep stays under 24h so it cannot outlive its own next scheduled run.
const INCREMENTAL_SWEEP_TIMEOUT = '23 hours'
const FULL_SWEEP_TIMEOUT = '47 hours'
const HEALTH_CHECK_TIMEOUT = '1 hour'

// One child per scoring thread, so no scoring waits for a thread inside its 25 minute deadline.
const SWEEP_CONCURRENCY = MAX_CONCURRENT_WORKERS

type ScheduleAction = Parameters<typeof svc.temporal.schedule.create>[0]['action']

async function createSchedule(scheduleId: string, cronExpression: string, action: ScheduleAction) {
  try {
    await svc.temporal.schedule.create({
      scheduleId,
      spec: {
        cronExpressions: [cronExpression],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 minute',
      },
      action,
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info(`Schedule ${scheduleId} already registered in Temporal, reconciling its action.`)
      await svc.temporal.schedule.getHandle(scheduleId).update((prev) => ({ ...prev, action }))
    } else {
      throw new Error(err)
    }
  }
}

export const scheduleDocsReadinessSweeps = async () => {
  await createSchedule('docsReadinessFullSweep', '0 2 1 * *', {
    type: 'startWorkflow',
    workflowType: runDocsReadinessSweep,
    taskQueue: DOCS_READINESS_TASK_QUEUE,
    workflowExecutionTimeout: FULL_SWEEP_TIMEOUT,
    retry: { initialInterval: '15 seconds', backoffCoefficient: 2, maximumAttempts: 3 },
    args: [{ mode: 'full', scope: 'lf', concurrency: SWEEP_CONCURRENCY }],
  })
  await createSchedule('docsReadinessIncrementalSweep', '0 3 * * *', {
    type: 'startWorkflow',
    workflowType: runDocsReadinessSweep,
    taskQueue: DOCS_READINESS_TASK_QUEUE,
    workflowExecutionTimeout: INCREMENTAL_SWEEP_TIMEOUT,
    retry: { initialInterval: '15 seconds', backoffCoefficient: 2, maximumAttempts: 3 },
    args: [{ mode: 'incremental', scope: 'lf', concurrency: SWEEP_CONCURRENCY }],
  })
  await createSchedule('docsReadinessIncrementalSweepHealthCheck', '0 6 * * *', {
    type: 'startWorkflow',
    workflowType: checkDocsReadinessSweepHealth,
    taskQueue: DOCS_READINESS_TASK_QUEUE,
    workflowExecutionTimeout: HEALTH_CHECK_TIMEOUT,
    retry: { initialInterval: '15 seconds', backoffCoefficient: 2, maximumAttempts: 3 },
    args: [],
  })
}
