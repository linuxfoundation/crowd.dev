import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../service'
import {
  backfillDailyDownloads,
  backfillLast30dHistory,
  ingestNpmPackages,
  refreshLatestLast30dDownloads,
} from '../workflows'

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
        taskQueue: 'npm-worker',
        workflowRunTimeout: '24 hours',
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
        taskQueue: 'npm-worker',
        workflowRunTimeout: '24 hours',
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

// Breadth: refresh the current 30-day window for every package, monthly on the 1st. Fast and
// mirrored to packages.downloads_last_30d — the product-critical number lands across the whole universe.
export async function scheduleLatestLast30dRefresh(): Promise<void> {
  const { temporal } = svc
  if (!temporal) throw new Error('Temporal client not initialized')

  try {
    await temporal.schedule.create({
      scheduleId: 'npm-last-30d-latest-refresh',
      spec: {
        cronExpressions: ['0 4 1 * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 day',
      },
      action: {
        type: 'startWorkflow',
        workflowType: refreshLatestLast30dDownloads,
        taskQueue: 'npm-worker',
        workflowRunTimeout: '24 hours',
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
      svc.log.info('Schedule npm-last-30d-latest-refresh already registered.')
    } else {
      throw err
    }
  }
}

// Depth: backfill older 30-day history in the background, daily. Independent of the monthly
// breadth refresh (overlap SKIP), so a long initial backfill never blocks the latest-window run.
export async function scheduleLast30dHistoryBackfill(): Promise<void> {
  const { temporal } = svc
  if (!temporal) throw new Error('Temporal client not initialized')

  try {
    await temporal.schedule.create({
      scheduleId: 'npm-last-30d-history-backfill',
      spec: {
        cronExpressions: ['30 4 * * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 hour',
      },
      action: {
        type: 'startWorkflow',
        workflowType: backfillLast30dHistory,
        taskQueue: 'npm-worker',
        workflowRunTimeout: '24 hours',
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
      svc.log.info('Schedule npm-last-30d-history-backfill already registered.')
    } else {
      throw err
    }
  }
}
