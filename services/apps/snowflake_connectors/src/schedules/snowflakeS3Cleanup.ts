import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { SlackChannel, SlackPersona, sendSlackNotification } from '@crowd/slack'

import { svc } from '../main'
import { snowflakeS3CleanupScheduler } from '../workflows'

export const scheduleSnowflakeS3Cleanup = async () => {
  try {
    await svc.temporal.schedule.create({
      scheduleId: 'snowflake-s3-cleanup',
      spec: {
        // Run at 02:00 every day
        cronExpressions: ['00 2 * * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 minute',
      },
      action: {
        type: 'startWorkflow',
        workflowType: snowflakeS3CleanupScheduler,
        taskQueue: 'snowflakeConnectors',
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
      svc.log.info('Cleanup schedule already registered in Temporal.')
      svc.log.info('Configuration may have changed since. Please make sure they are in sync.')
    } else {
      svc.log.error({ err }, 'Failed to create snowflake-s3-cleanup schedule')
      sendSlackNotification(
        SlackChannel.CDP_INTEGRATIONS_ALERTS,
        SlackPersona.ERROR_REPORTER,
        'Snowflake S3 Cleanup Schedule Failed',
        `Failed to create the \`snowflake-s3-cleanup\` Temporal schedule.\n\n*Error:* ${err.message || err}`,
      )
    }
  }
}
