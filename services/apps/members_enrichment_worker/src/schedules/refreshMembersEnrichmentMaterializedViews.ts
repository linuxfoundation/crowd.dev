import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../service'
import { refreshMemberEnrichmentMaterializedViews } from '../workflows'

export const scheduleRefreshMembersEnrichmentMaterializedViews = async () => {
  try {
    await svc.temporal.schedule.create({
      scheduleId: 'refresh-members-enrichment-materialized-views',
      spec: {
        cronExpressions: ['0 5 * * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 minute',
      },
      action: {
        type: 'startWorkflow',
        workflowType: refreshMemberEnrichmentMaterializedViews,
        taskQueue: 'members-enrichment',
        retry: {
          initialInterval: '15 seconds',
          backoffCoefficient: 2,
          maximumAttempts: 3,
        },
      },
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info('Schedule already registered in Temporal.')
      svc.log.info('Configuration may have changed since. Please make sure they are in sync.')
    } else {
      throw new Error(err)
    }
  }
}
