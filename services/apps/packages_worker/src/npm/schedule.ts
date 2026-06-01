import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../service'
import { backfillDailyDownloads, ingestNpmPackages, refreshLast30dDownloads } from '../workflows'

export async function scheduleNpmIngest(): Promise<void> {
  const { temporal } = svc
  if (!temporal) throw new Error('Temporal client not initialized')

  try {
    await temporal.schedule.create({
      scheduleId: 'npm-registry-ingest',
      spec: {
        cronExpressions: ['15 3 * * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 hour',
      },
      action: {
        type: 'startWorkflow',
        workflowType: ingestNpmPackages,
        taskQueue: 'packages-worker',
        workflowExecutionTimeout: '1 hour',
        retry: {
          initialInterval: '30 seconds',
          backoffCoefficient: 2,
          maximumAttempts: 3,
        },
        args: [],
      },
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info('Schedule npm-registry-ingest already registered.')
    } else {
      throw err
    }
  }
}

export async function scheduleDailyDownloadsBackfill(): Promise<void> {
  const { temporal } = svc
  if (!temporal) throw new Error('Temporal client not initialized')

  try {
    await temporal.schedule.create({
      scheduleId: 'npm-daily-downloads-backfill',
      spec: {
        cronExpressions: ['30 3 * * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 hour',
      },
      action: {
        type: 'startWorkflow',
        workflowType: backfillDailyDownloads,
        taskQueue: 'packages-worker',
        workflowExecutionTimeout: '6 hours',
        retry: {
          initialInterval: '30 seconds',
          backoffCoefficient: 2,
          maximumAttempts: 3,
        },
        args: [],
      },
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info('Schedule npm-daily-downloads-backfill already registered.')
    } else {
      throw err
    }
  }
}

export async function scheduleLast30dDownloadsRefresh(): Promise<void> {
  const { temporal } = svc
  if (!temporal) throw new Error('Temporal client not initialized')

  try {
    await temporal.schedule.create({
      scheduleId: 'npm-last-30d-downloads-refresh',
      spec: {
        cronExpressions: ['0 4 1 * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 day',
      },
      action: {
        type: 'startWorkflow',
        workflowType: refreshLast30dDownloads,
        taskQueue: 'packages-worker',
        workflowExecutionTimeout: '6 hours',
        retry: {
          initialInterval: '30 seconds',
          backoffCoefficient: 2,
          maximumAttempts: 3,
        },
        args: [],
      },
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info('Schedule npm-last-30d-downloads-refresh already registered.')
    } else {
      throw err
    }
  }
}
