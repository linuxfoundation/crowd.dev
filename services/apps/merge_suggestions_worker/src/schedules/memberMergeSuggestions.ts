import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { DEFAULT_TENANT_ID } from '@crowd/common'

import { svc } from '../main'
import { generateMemberMergeSuggestions } from '../workflows/generateMemberMergeSuggestions'

export const scheduleGenerateMemberMergeSuggestions = async () => {
  try {
    await svc.temporal.schedule.create({
      scheduleId: 'member-merge-suggestions',
      spec: {
        cronExpressions: ['0 */2 * * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.BUFFER_ONE,
        catchupWindow: '1 minute',
      },
      action: {
        type: 'startWorkflow',
        workflowType: generateMemberMergeSuggestions,
        taskQueue: 'merge-suggestions',
        args: [
          {
            tenantId: DEFAULT_TENANT_ID,
          },
        ],
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
