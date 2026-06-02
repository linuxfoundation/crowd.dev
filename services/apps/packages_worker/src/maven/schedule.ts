import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../service'
import { mavenCriticalWorkflow, mavenNonCriticalWorkflow } from '../workflows'

export async function scheduleMavenCritical(): Promise<void> {
  const { temporal } = svc
  if (!temporal) throw new Error('Temporal client not initialized')

  try {
    await temporal.schedule.create({
      scheduleId: 'maven-critical',
      spec: {
        cronExpressions: ['*/5 * * * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 hour',
      },
      action: {
        type: 'startWorkflow',
        workflowType: mavenCriticalWorkflow,
        taskQueue: 'packages-worker',
        workflowExecutionTimeout: '15 minutes',
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
      svc.log.info('Schedule maven-critical already registered.')
    } else {
      throw err
    }
  }
}

export async function scheduleMavenNonCritical(): Promise<void> {
  const { temporal } = svc
  if (!temporal) throw new Error('Temporal client not initialized')

  try {
    await temporal.schedule.create({
      scheduleId: 'maven-non-critical',
      spec: {
        cronExpressions: ['*/10 * * * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 hour',
      },
      action: {
        type: 'startWorkflow',
        workflowType: mavenNonCriticalWorkflow,
        taskQueue: 'packages-worker',
        workflowExecutionTimeout: '5 minutes',
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
      svc.log.info('Schedule maven-non-critical already registered.')
    } else {
      throw err
    }
  }
}
