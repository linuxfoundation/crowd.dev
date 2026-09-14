import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../service'

import { sweepPackageRepoConfidence } from './workflows'

export async function schedulePackageRepoConfidenceSweep(): Promise<void> {
  const { temporal } = svc
  if (!temporal) throw new Error('Temporal client not initialized')

  try {
    await temporal.schedule.create({
      scheduleId: 'package-repo-confidence-sweep-daily',
      spec: {
        cronExpressions: ['0 5 * * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 hour',
      },
      action: {
        type: 'startWorkflow',
        workflowType: sweepPackageRepoConfidence,
        taskQueue: 'bq-dataset-ingest',
        // Covers the activity budget: one 6h sweep attempt + 30m report.
        workflowRunTimeout: '7 hours',
        retry: { maximumAttempts: 1 },
        args: [],
      },
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info('Schedule package-repo-confidence-sweep-daily already registered.')
    } else {
      throw err
    }
  }
}
