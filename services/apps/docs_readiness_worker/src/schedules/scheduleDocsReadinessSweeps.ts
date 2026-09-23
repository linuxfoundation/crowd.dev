import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../main'
import { DOCS_READINESS_TASK_QUEUE } from '../types'
import { checkDocsReadinessSweepHealth, runDocsReadinessSweep } from '../workflows'

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
      svc.log.info(`Schedule ${scheduleId} already registered in Temporal.`)
      svc.log.info('Configuration may have changed since. Please make sure they are in sync.')
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
    retry: { initialInterval: '15 seconds', backoffCoefficient: 2, maximumAttempts: 3 },
    args: [{ mode: 'full', scope: 'lf' }],
  })
  await createSchedule('docsReadinessIncrementalSweep', '0 3 * * *', {
    type: 'startWorkflow',
    workflowType: runDocsReadinessSweep,
    taskQueue: DOCS_READINESS_TASK_QUEUE,
    retry: { initialInterval: '15 seconds', backoffCoefficient: 2, maximumAttempts: 3 },
    args: [{ mode: 'incremental', scope: 'lf' }],
  })
  await createSchedule('docsReadinessIncrementalSweepHealthCheck', '0 6 * * *', {
    type: 'startWorkflow',
    workflowType: checkDocsReadinessSweepHealth,
    taskQueue: DOCS_READINESS_TASK_QUEUE,
    retry: { initialInterval: '15 seconds', backoffCoefficient: 2, maximumAttempts: 3 },
    args: [],
  })
}
