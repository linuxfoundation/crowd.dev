import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../main'
import { inferMemberCountries } from '../workflows/member/inferMemberCountries'
import { inferOrganizationCountries } from '../workflows/organization/inferOrganizationCountries'

export const scheduleInferMemberCountries = async () => {
  try {
    await svc.temporal.schedule.create({
      scheduleId: 'infer-member-countries',
      spec: {
        cronExpressions: ['0 * * * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 minute',
      },
      action: {
        type: 'startWorkflow',
        workflowType: inferMemberCountries,
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

export const scheduleInferOrganizationCountries = async () => {
  try {
    await svc.temporal.schedule.create({
      scheduleId: 'infer-organization-countries',
      spec: {
        cronExpressions: ['0 * * * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 minute',
      },
      action: {
        type: 'startWorkflow',
        workflowType: inferOrganizationCountries,
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
