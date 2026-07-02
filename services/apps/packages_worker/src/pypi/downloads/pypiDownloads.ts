import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../../service'

import { ingestPypiDownloadsDaily, ingestPypiDownloadsLast30d } from './ingestPypiDownloads'

// Last-30d downloads for all pypi packages. Runs on the 4th of the month (06:00 UTC) — a few
// days after the window's end (1st of the month) so the BigQuery partitions have settled. No
// args → the workflow processes only the latest window and mirrors it onto packages.
export async function schedulePypiDownloads30d(): Promise<void> {
  const { temporal } = svc
  if (!temporal) throw new Error('Temporal client not initialized')

  try {
    await temporal.schedule.create({
      scheduleId: 'pypi-downloads-30d-monthly',
      spec: { cronExpressions: ['0 6 4 * *'] },
      policies: { overlap: ScheduleOverlapPolicy.SKIP, catchupWindow: '1 hour' },
      action: {
        type: 'startWorkflow',
        workflowType: ingestPypiDownloadsLast30d,
        taskQueue: 'bq-dataset-ingest',
        workflowExecutionTimeout: '12 hours',
        retry: { initialInterval: '1 minute', backoffCoefficient: 2, maximumAttempts: 3 },
        args: [{}],
      },
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info('Schedule pypi-downloads-30d-monthly already registered.')
    } else {
      throw err
    }
  }
}

// Daily downloads for the critical pypi subset. Runs daily at 06:00 UTC; no args → the workflow
// scans the 2-day trailing re-scan window.
export async function schedulePypiDownloadsDaily(): Promise<void> {
  const { temporal } = svc
  if (!temporal) throw new Error('Temporal client not initialized')

  try {
    await temporal.schedule.create({
      scheduleId: 'pypi-downloads-daily',
      spec: { cronExpressions: ['0 6 * * *'] },
      policies: { overlap: ScheduleOverlapPolicy.SKIP, catchupWindow: '1 hour' },
      action: {
        type: 'startWorkflow',
        workflowType: ingestPypiDownloadsDaily,
        taskQueue: 'bq-dataset-ingest',
        workflowExecutionTimeout: '6 hours',
        retry: { initialInterval: '1 minute', backoffCoefficient: 2, maximumAttempts: 3 },
        args: [{}],
      },
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info('Schedule pypi-downloads-daily already registered.')
    } else {
      throw err
    }
  }
}
