import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../service'
import { mavenCriticalWorkflow } from '../workflows'

export async function scheduleMavenCritical(): Promise<void> {
  const { temporal } = svc
  if (!temporal) throw new Error('Temporal client not initialized')

  const scheduleOptions: Parameters<typeof temporal.schedule.create>[0] = {
    scheduleId: 'maven-critical',
    spec: {
      cronExpressions: ['*/1 * * * *'],
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
  }

  try {
    await temporal.schedule.create(scheduleOptions)
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      // Schedule exists → delete and recreate so cron/spec changes take effect on
      // restart (schedule.create is a no-op when the id exists → it would keep the old cron).
      await temporal.schedule.getHandle('maven-critical').delete()
      await temporal.schedule.create(scheduleOptions)
      svc.log.info('Schedule maven-critical recreated (cron synced).')
    } else {
      throw err
    }
  }
}

// export async function scheduleMavenNonCritical(): Promise<void> {
//   const { temporal } = svc
//   if (!temporal) throw new Error('Temporal client not initialized')

//   try {
//     await temporal.schedule.create({
//       scheduleId: 'maven-non-critical',
//       spec: {
//         cronExpressions: ['*/10 * * * *'],
//       },
//       policies: {
//         overlap: ScheduleOverlapPolicy.SKIP,
//         catchupWindow: '1 hour',
//       },
//       action: {
//         type: 'startWorkflow',
//         workflowType: mavenNonCriticalWorkflow,
//         taskQueue: 'packages-worker',
//         workflowExecutionTimeout: '5 minutes',
//         retry: {
//           initialInterval: '30 seconds',
//           backoffCoefficient: 2,
//           maximumAttempts: 3,
//         },
//         args: [],
//       },
//     })
//   } catch (err) {
//     if (err instanceof ScheduleAlreadyRunning) {
//       svc.log.info('Schedule maven-non-critical already registered.')
//     } else {
//       throw err
//     }
// }
// }
