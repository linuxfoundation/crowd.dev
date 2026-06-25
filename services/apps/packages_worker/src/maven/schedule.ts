import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../service'
import { ingestMavenPackages } from '../workflows'

const LEGACY_SCHEDULE_ID = 'maven-critical'

export async function scheduleMavenIngestion(): Promise<void> {
  const { temporal } = svc
  if (!temporal) throw new Error('Temporal client not initialized')

  try {
    await temporal.schedule.getHandle(LEGACY_SCHEDULE_ID).delete()
    svc.log.info({ scheduleId: LEGACY_SCHEDULE_ID }, 'Deleted legacy schedule.')
  } catch (err) {
    svc.log.warn(
      { err, scheduleId: LEGACY_SCHEDULE_ID },
      'Failed to delete legacy schedule (may not exist).',
    )
  }

  try {
    await temporal.schedule.create({
      scheduleId: 'maven-ingestion',
      spec: {
        cronExpressions: ['0 9 * * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 hour',
      },
      action: {
        type: 'startWorkflow',
        workflowType: ingestMavenPackages,
        workflowId: 'maven-daily-enrichment',
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
      svc.log.info('Schedule maven-ingestion already exists, skipping creation.')
    } else {
      throw err
    }
  }
}
