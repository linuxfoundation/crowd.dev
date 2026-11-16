import {
  ScheduleAlreadyRunning,
  type ScheduleOptionsAction,
  ScheduleOverlapPolicy,
  type Workflow,
} from '@temporalio/client'

import { svc } from '../service'
import {
  backfillDailyDownloads,
  backfillLast30dHistory,
  ingestNpmPackages,
  refreshLatestLast30dDownloads,
} from '../workflows'

interface NpmSchedule {
  scheduleId: string
  cron: string
  catchupWindow: '1 hour' | '1 day'
  workflowType: Workflow
}

async function createOrReconcileSchedule({
  scheduleId,
  cron,
  catchupWindow,
  workflowType,
}: NpmSchedule): Promise<void> {
  const { temporal } = svc
  if (!temporal) throw new Error('Temporal client not initialized')

  const spec = { cronExpressions: [cron] }
  const policies = { overlap: ScheduleOverlapPolicy.SKIP, catchupWindow }
  const action: ScheduleOptionsAction = {
    type: 'startWorkflow',
    workflowType,
    taskQueue: 'npm-worker',
    workflowRunTimeout: '24 hours',
    retry: {
      initialInterval: '30 seconds',
      backoffCoefficient: 2,
      maximumAttempts: 3,
    },
    args: [],
  }

  try {
    await temporal.schedule.create({ scheduleId, spec, policies, action })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info(`Schedule ${scheduleId} already exists, reconciling spec and action.`)
      // The spec is replaced wholesale so a drifted interval or calendar spec cannot fire
      // alongside the cron; spreading prev keeps a paused schedule paused.
      await temporal.schedule.getHandle(scheduleId).update((prev) => ({
        ...prev,
        spec,
        policies: { ...prev.policies, ...policies },
        action,
      }))
    } else {
      throw err
    }
  }
}

export async function scheduleNpmIngest(): Promise<void> {
  await createOrReconcileSchedule({
    scheduleId: 'npm-registry-ingest',
    cron: '15 3 * * *',
    catchupWindow: '1 hour',
    workflowType: ingestNpmPackages,
  })
}

export async function scheduleDailyDownloadsBackfill(): Promise<void> {
  await createOrReconcileSchedule({
    scheduleId: 'npm-daily-downloads-backfill',
    cron: '30 3 * * *',
    catchupWindow: '1 hour',
    workflowType: backfillDailyDownloads,
  })
}

// Breadth: refresh the current 30-day window for every package, monthly on the 1st. Fast and
// mirrored to packages.downloads_last_30d — the product-critical number lands across the whole universe.
export async function scheduleLatestLast30dRefresh(): Promise<void> {
  await createOrReconcileSchedule({
    scheduleId: 'npm-last-30d-latest-refresh',
    cron: '0 4 1 * *',
    catchupWindow: '1 day',
    workflowType: refreshLatestLast30dDownloads,
  })
}

// Depth: backfill older 30-day history in the background, daily. Independent of the monthly
// breadth refresh (overlap SKIP), so a long initial backfill never blocks the latest-window run.
export async function scheduleLast30dHistoryBackfill(): Promise<void> {
  await createOrReconcileSchedule({
    scheduleId: 'npm-last-30d-history-backfill',
    cron: '30 4 * * *',
    catchupWindow: '1 hour',
    workflowType: backfillLast30dHistory,
  })
}
