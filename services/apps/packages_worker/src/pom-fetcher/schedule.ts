import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../service'
import { pomFetcherWorkflow } from '../workflows'

export async function schedulePomFetcher(): Promise<void> {
  const { temporal } = svc
  if (!temporal) throw new Error('Temporal client not initialized')

  try {
    await temporal.schedule.create({
      scheduleId: 'maven-pom-fetcher',
      spec: {
        // Run daily at 4am UTC — off-peak, after nightly GitHub enrichment completes
        cronExpressions: ['0 4 * * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 hour',
      },
      action: {
        type: 'startWorkflow',
        workflowType: pomFetcherWorkflow,
        taskQueue: 'packages-worker',
        workflowExecutionTimeout: '12 hours',
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
      svc.log.info('Schedule maven-pom-fetcher already registered.')
    } else {
      throw err
    }
  }
}
