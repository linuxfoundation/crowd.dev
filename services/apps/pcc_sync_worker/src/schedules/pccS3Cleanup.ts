import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { SlackChannel, SlackPersona, sendSlackNotification } from '@crowd/slack'

import { svc } from '../main'
import { pccS3CleanupScheduler } from '../workflows'

export const schedulePccS3Cleanup = async () => {
  try {
    await svc.temporal.schedule.create({
      scheduleId: 'pcc-s3-cleanup',
      spec: {
        // Run at 03:00 every day
        cronExpressions: ['00 3 * * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 minute',
      },
      action: {
        type: 'startWorkflow',
        workflowType: pccS3CleanupScheduler,
        taskQueue: 'pccSync',
        retry: {
          initialInterval: '15 seconds',
          backoffCoefficient: 2,
          maximumAttempts: 3,
        },
        args: [],
      },
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info('PCC cleanup schedule already registered in Temporal.')
      svc.log.info('Configuration may have changed since. Please make sure they are in sync.')
    } else {
      svc.log.error({ err }, 'Failed to create pcc-s3-cleanup schedule')
      sendSlackNotification(
        SlackChannel.CDP_INTEGRATIONS_ALERTS,
        SlackPersona.ERROR_REPORTER,
        'PCC S3 Cleanup Schedule Failed',
        `Failed to create the \`pcc-s3-cleanup\` Temporal schedule.\n\n*Error:* ${err instanceof Error ? err.message : String(err)}`,
      )
    }
  }
}
