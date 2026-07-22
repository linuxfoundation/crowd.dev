import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

// Cron minutes deliberately off :00 per Packagist crawler guidelines.
// The weekly metadata drain has no cron: seedPackagistPackages starts it as a
// child workflow once the seed completes, so sequencing is an event, not a
// clock offset.
export const PACKAGIST_CRONS = {
  seed: '17 2 * * 0',
  downloads30d: '53 3 1 * *',
  // Late in the UTC day on purpose: Packagist's `daily` figure is mostly real data by
  // 22:23 (vs. mostly borrowed from yesterday earlier on), with buffer before midnight.
  downloadsDaily: '23 22 * * *',
}

// Workflow types by name (not function reference) so this module doesn't pull the
// whole workflows index into consumers that only need the crons.
const SCHEDULES = [
  {
    scheduleId: 'packagist-seed',
    cron: PACKAGIST_CRONS.seed,
    workflowType: 'seedPackagistPackages',
    args: [] as unknown[],
  },
  {
    scheduleId: 'packagist-downloads-30d',
    cron: PACKAGIST_CRONS.downloads30d,
    workflowType: 'ingestPackagistDownloads30d',
    args: [{}] as unknown[],
  },
  {
    scheduleId: 'packagist-downloads-daily',
    cron: PACKAGIST_CRONS.downloadsDaily,
    workflowType: 'ingestPackagistDownloadsDaily',
    args: [{}] as unknown[],
  },
]

export async function schedulePackagistIngest(): Promise<void> {
  // svc is imported lazily: its module graph can't load under vitest, and the wiring
  // test imports PACKAGIST_CRONS from this file. The .js extension is required by
  // node16 module resolution for dynamic imports.
  const { svc } = await import('../service.js')

  const { temporal } = svc
  if (!temporal) throw new Error('Temporal client not initialized')

  for (const schedule of SCHEDULES) {
    try {
      await temporal.schedule.create({
        scheduleId: schedule.scheduleId,
        spec: {
          cronExpressions: [schedule.cron],
        },
        policies: {
          overlap: ScheduleOverlapPolicy.SKIP,
          catchupWindow: '1 hour',
        },
        action: {
          type: 'startWorkflow',
          workflowType: schedule.workflowType,
          taskQueue: 'packagist-worker',
          workflowRunTimeout: '24 hours',
          retry: {
            initialInterval: '30 seconds',
            backoffCoefficient: 2,
            maximumAttempts: 3,
          },
          args: schedule.args,
        },
      })
    } catch (err) {
      if (err instanceof ScheduleAlreadyRunning) {
        svc.log.info(`Schedule ${schedule.scheduleId} already registered.`)
      } else {
        throw err
      }
    }
  }
}
