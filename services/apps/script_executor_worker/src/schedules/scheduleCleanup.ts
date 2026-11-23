import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../main'
import { cleanupMembers } from '../workflows/cleanup/members'
import { cleanupMemberSegmentsAgg } from '../workflows/cleanup/memberSegmentsAgg'
import { cleanupOrganizations } from '../workflows/cleanup/organizations'
import { cleanupOrganizationSegmentAgg } from '../workflows/cleanup/organizationSegmentsAgg'

export const scheduleMembersCleanup = async () => {
  try {
    await svc.temporal.schedule.create({
      scheduleId: 'cleanupMembers',
      spec: {
        cronExpressions: ['0 9 * * 3'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.BUFFER_ONE,
        catchupWindow: '1 minute',
      },
      action: {
        type: 'startWorkflow',
        workflowType: cleanupMembers,
        taskQueue: 'script-executor',
        retry: {
          initialInterval: '15 seconds',
          backoffCoefficient: 2,
          maximumAttempts: 3,
        },
        args: [
          {
            batchSize: 500,
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

export const scheduleOrganizationsCleanup = async () => {
  try {
    await svc.temporal.schedule.create({
      scheduleId: 'cleanupOrganizations',
      spec: {
        cronExpressions: ['0 9 * * 3'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.BUFFER_ONE,
        catchupWindow: '1 minute',
      },
      action: {
        type: 'startWorkflow',
        workflowType: cleanupOrganizations,
        taskQueue: 'script-executor',
        retry: {
          initialInterval: '15 seconds',
          backoffCoefficient: 2,
          maximumAttempts: 3,
        },
        args: [
          {
            batchSize: 500,
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

// after the daily aggregate sinks (06:00 and 06:05 UTC), which send keys that no longer exist
// with activityCount 0
const SEGMENTS_AGG_CLEANUP_SPEC = { cronExpressions: ['0 7 * * *'] }

export const scheduleMemberSegmentsAggCleanup = async () => {
  try {
    await svc.temporal.schedule.create({
      scheduleId: 'cleanupMemberSegmentsAgg',
      spec: SEGMENTS_AGG_CLEANUP_SPEC,
      policies: {
        overlap: ScheduleOverlapPolicy.BUFFER_ONE,
        catchupWindow: '1 minute',
      },
      action: {
        type: 'startWorkflow',
        workflowType: cleanupMemberSegmentsAgg,
        taskQueue: 'script-executor',
        retry: {
          initialInterval: '15 seconds',
          backoffCoefficient: 2,
          maximumAttempts: 3,
        },
        args: [
          {
            batchSize: 500,
          },
        ],
      },
    })
    svc.log.info('Schedule for member segments agg cleanup created successfully!')
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      await svc.temporal.schedule
        .getHandle('cleanupMemberSegmentsAgg')
        .update((prev) => ({ ...prev, spec: SEGMENTS_AGG_CLEANUP_SPEC }))
      svc.log.info(
        'Schedule cleanupMemberSegmentsAgg already registered in Temporal, spec updated.',
      )
    } else {
      svc.log.error({ err }, 'Error creating schedule for member segments agg cleanup')
      throw new Error(err)
    }
  }
}

export const scheduleOrganizationSegmentAggCleanup = async () => {
  try {
    await svc.temporal.schedule.create({
      scheduleId: 'cleanupOrganizationSegmentAgg',
      spec: SEGMENTS_AGG_CLEANUP_SPEC,
      policies: {
        overlap: ScheduleOverlapPolicy.BUFFER_ONE,
        catchupWindow: '1 minute',
      },
      action: {
        type: 'startWorkflow',
        workflowType: cleanupOrganizationSegmentAgg,
        taskQueue: 'script-executor',
        retry: {
          initialInterval: '15 seconds',
          backoffCoefficient: 2,
          maximumAttempts: 3,
        },
        args: [
          {
            batchSize: 500,
          },
        ],
      },
    })
    svc.log.info('Schedule for organization segment agg cleanup created successfully!')
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      await svc.temporal.schedule
        .getHandle('cleanupOrganizationSegmentAgg')
        .update((prev) => ({ ...prev, spec: SEGMENTS_AGG_CLEANUP_SPEC }))
      svc.log.info(
        'Schedule cleanupOrganizationSegmentAgg already registered in Temporal, spec updated.',
      )
    } else {
      svc.log.error({ err }, 'Error creating schedule for organization segment agg cleanup')
      throw new Error(err)
    }
  }
}
