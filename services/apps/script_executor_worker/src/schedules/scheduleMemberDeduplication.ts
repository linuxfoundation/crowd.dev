import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../main'
import { findAndMergeMembersWithSameVerifiedEmailsInDifferentPlatforms } from '../workflows/findAndMergeMembersWithSameVerifiedEmailsInDifferentPlatforms'

export const scheduleMergeMembersWithSameVerifiedEmails = async () => {
  try {
    await svc.temporal.schedule.create({
      scheduleId: 'mergeMembersWithSameVerifiedEmails',
      spec: {
        cronExpressions: ['0 3 * * 0'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 minute',
      },
      action: {
        type: 'startWorkflow',
        workflowType: findAndMergeMembersWithSameVerifiedEmailsInDifferentPlatforms,
        taskQueue: 'script-executor',
        retry: {
          initialInterval: '15 seconds',
          backoffCoefficient: 2,
          maximumAttempts: 3,
        },
        args: [{}],
      },
    })
    svc.log.info('Schedule for merging members with same verified emails created successfully!')
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info('Schedule mergeMembersWithSameVerifiedEmails already registered in Temporal.')
      svc.log.info('Configuration may have changed since. Please make sure they are in sync.')
    } else {
      svc.log.error({ err }, 'Error creating schedule for member email deduplication')
      throw new Error(err)
    }
  }
}
