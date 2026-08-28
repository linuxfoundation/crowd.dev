import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../main'
import { IOnboardProjectsInput, onboardProjects } from '../workflows'

const ONBOARDING_ARGS: IOnboardProjectsInput = {
  batchSize: 50,
}

export const scheduleProjectsOnboarding = async () => {
  svc.log.info('Scheduling projects onboarding')

  try {
    await svc.temporal.schedule.create({
      scheduleId: 'projectsOnboarding',
      spec: {
        // Daily: catches up on whatever landed in 'onboard' state, independent of the evaluation schedule's timing.
        cronExpressions: ['0 8 * * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 minute',
      },
      action: {
        type: 'startWorkflow',
        workflowType: onboardProjects,
        taskQueue: 'automatic-onboarding',
        args: [ONBOARDING_ARGS],
        // 50 projects × up to ~2min per attempt, up to 2 attempts each = ~3.3h worst case; set ceiling with margin.
        workflowExecutionTimeout: '6 hours',
        retry: {
          initialInterval: '30 seconds',
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
