import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../main'
import { githubLifecycleSweep } from '../workflows/githubLifecycleSweep'

export async function scheduleGithubLifecycleSweep(): Promise<void> {
  try {
    await svc.temporal.schedule.create({
      scheduleId: 'connectors-github-lifecycle-sweep',
      spec: {
        intervals: [{ every: '1 hour' }],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 hour',
      },
      action: {
        type: 'startWorkflow',
        workflowType: githubLifecycleSweep,
        taskQueue: 'connectors',
        workflowExecutionTimeout: '4 hours',
      },
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info('Github lifecycle sweep schedule already registered in Temporal.')
    } else {
      throw new Error(err)
    }
  }
}
