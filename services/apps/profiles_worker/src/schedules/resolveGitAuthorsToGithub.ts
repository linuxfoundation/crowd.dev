import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../main'
import { resolveGitAuthorsToGithub } from '../workflows/member/resolveGitAuthorsToGithub'

export const scheduleResolveGitAuthorsToGithub = async () => {
  try {
    await svc.temporal.schedule.create({
      scheduleId: 'resolve-git-authors-to-github',
      spec: {
        cronExpressions: ['0 2 * * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.BUFFER_ONE,
        catchupWindow: '1 minute',
      },
      action: {
        type: 'startWorkflow',
        workflowType: resolveGitAuthorsToGithub,
        taskQueue: 'profiles',
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
