import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../main'
import { DOCS_READINESS_TASK_QUEUE } from '../types'
import { runDocsReadinessSweep } from '../workflows'

async function createDocsReadinessSchedule(
  scheduleId: string,
  cronExpression: string,
  mode: 'full' | 'incremental',
) {
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
      action: {
        type: 'startWorkflow',
        workflowType: runDocsReadinessSweep,
        taskQueue: DOCS_READINESS_TASK_QUEUE,
        retry: {
          initialInterval: '15 seconds',
          backoffCoefficient: 2,
          maximumAttempts: 3,
        },
        args: [{ mode, scope: 'lf' }],
      },
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
  await createDocsReadinessSchedule('docsReadinessFullSweep', '0 2 1 * *', 'full')
  await createDocsReadinessSchedule('docsReadinessIncrementalSweep', '0 3 * * *', 'incremental')
}
