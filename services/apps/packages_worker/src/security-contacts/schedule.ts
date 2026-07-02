import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../service'
import { ingestSecurityContacts } from '../workflows'

export async function scheduleSecurityContactsIngestion(): Promise<void> {
  const { temporal } = svc
  if (!temporal) throw new Error('Temporal client not initialized')

  try {
    await temporal.schedule.create({
      scheduleId: 'security-contacts-ingestion',
      spec: {
        cronExpressions: ['0 6 * * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 hour',
      },
      action: {
        type: 'startWorkflow',
        workflowType: ingestSecurityContacts,
        workflowId: 'security-contacts-daily',
        taskQueue: 'packages-worker',
        workflowRunTimeout: '24 hours',
        retry: {
          initialInterval: '30 seconds',
          backoffCoefficient: 2,
          maximumAttempts: 3,
        },
        args: [],
      },
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info('Schedule security-contacts-ingestion already exists, skipping creation.')
    } else {
      throw err
    }
  }
}
