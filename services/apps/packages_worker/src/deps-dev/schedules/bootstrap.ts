import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../../service'
import { bootstrapOsspckgs } from '../workflows'

export async function scheduleOsspckgsBootstrap(): Promise<void> {
  const { temporal } = svc
  if (!temporal) throw new Error('Temporal client not initialized')

  try {
    await temporal.schedule.create({
      scheduleId: 'osspckgs-bootstrap-weekly',
      spec: {
        cronExpressions: ['0 2 * * 1'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 hour',
      },
      action: {
        type: 'startWorkflow',
        workflowType: bootstrapOsspckgs,
        taskQueue: 'bq-dataset-ingest',
        workflowExecutionTimeout: '12 hours',
        retry: {
          initialInterval: '1 minute',
          backoffCoefficient: 2,
          maximumAttempts: 3,
        },
        args: [{ mode: 'incremental' }],
      },
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info('Schedule osspckgs-bootstrap-weekly already registered.')
    } else {
      throw err
    }
  }
}
