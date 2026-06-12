import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { cleanupOsspckgs } from '../deps-dev/workflows'
import { svc } from '../service'

export async function scheduleOsspckgsCleanup(): Promise<void> {
  const { temporal } = svc
  if (!temporal) throw new Error('Temporal client not initialized')

  try {
    await temporal.schedule.create({
      scheduleId: 'osspckgs-cleanup-daily',
      spec: {
        cronExpressions: ['30 3 * * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '30 minutes',
      },
      action: {
        type: 'startWorkflow',
        workflowType: cleanupOsspckgs,
        taskQueue: 'bq-dataset-ingest',
        workflowExecutionTimeout: '1 hour',
        retry: {
          initialInterval: '1 minute',
          backoffCoefficient: 2,
          maximumAttempts: 3,
        },
        args: [],
      },
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info('Schedule osspckgs-cleanup-daily already registered.')
    } else {
      throw err
    }
  }
}
