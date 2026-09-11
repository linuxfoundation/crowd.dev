import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../main'
import { spawnSubprojectMemberMergeSuggestions } from '../workflows/spawnSubprojectMemberMergeSuggestions'

export const scheduleSubprojectMemberMergeSuggestions = async () => {
  try {
    await svc.temporal.schedule.create({
      scheduleId: 'subproject-member-merge-suggestions',
      spec: {
        cronExpressions: ['0 6 * * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 minute',
      },
      action: {
        type: 'startWorkflow',
        workflowType: spawnSubprojectMemberMergeSuggestions,
        taskQueue: 'merge-suggestions',
        args: [],
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
