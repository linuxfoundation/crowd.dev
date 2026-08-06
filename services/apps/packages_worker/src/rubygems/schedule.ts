import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../service'
import { ingestRubyGemsCriticalDetails, ingestRubyGemsPackages } from '../workflows'

export async function scheduleRubyGemsIngestion(): Promise<void> {
  const { temporal } = svc
  if (!temporal) throw new Error('Temporal client not initialized')

  try {
    await temporal.schedule.create({
      scheduleId: 'rubygems-registry-ingest',
      spec: {
        cronExpressions: ['0 7 * * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 hour',
      },
      action: {
        type: 'startWorkflow',
        workflowType: ingestRubyGemsPackages,
        workflowId: 'rubygems-daily-core',
        taskQueue: 'rubygems-worker',
        workflowRunTimeout: '24 hours',
        retry: {
          initialInterval: '30 seconds',
          backoffCoefficient: 2,
          maximumAttempts: 5,
        },
        args: [],
      },
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info('Schedule rubygems-registry-ingest already exists, skipping creation.')
    } else {
      throw err
    }
  }
}

export async function scheduleRubyGemsCriticalIngestion(): Promise<void> {
  const { temporal } = svc
  if (!temporal) throw new Error('Temporal client not initialized')

  try {
    await temporal.schedule.create({
      scheduleId: 'rubygems-critical-ingest',
      spec: {
        cronExpressions: ['0 9 * * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 hour',
      },
      action: {
        type: 'startWorkflow',
        workflowType: ingestRubyGemsCriticalDetails,
        workflowId: 'rubygems-daily-critical',
        taskQueue: 'rubygems-worker',
        workflowRunTimeout: '24 hours',
        retry: {
          initialInterval: '30 seconds',
          backoffCoefficient: 2,
          maximumAttempts: 5,
        },
        args: [],
      },
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info('Schedule rubygems-critical-ingest already exists, skipping creation.')
    } else {
      throw err
    }
  }
}
