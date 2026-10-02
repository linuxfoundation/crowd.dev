import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../service'
import { syncGovernanceFileContributors } from '../workflows'

const SCHEDULE_ID = 'governance-file-contributors-sync'
const WORKFLOW_EXECUTION_TIMEOUT = '6 hours'

function scheduleAction() {
  return {
    type: 'startWorkflow' as const,
    workflowType: syncGovernanceFileContributors,
    workflowId: 'governance-file-contributors-daily',
    taskQueue: 'security-contacts-worker',
    workflowExecutionTimeout: WORKFLOW_EXECUTION_TIMEOUT,
    retry: {
      initialInterval: '30 seconds',
      backoffCoefficient: 2,
      maximumAttempts: 3,
    },
    args: [] as [],
  }
}

export async function scheduleGovernanceFileContributorsSync(): Promise<void> {
  const { temporal } = svc
  if (!temporal) throw new Error('Temporal client not initialized')

  try {
    await temporal.schedule.create({
      scheduleId: SCHEDULE_ID,
      spec: {
        cronExpressions: ['0 5 * * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 hour',
      },
      action: scheduleAction(),
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info(`Schedule ${SCHEDULE_ID} already exists, reconciling action.`)
      const handle = temporal.schedule.getHandle(SCHEDULE_ID)
      await handle.update((prev) => ({
        ...prev,
        policies: {
          ...prev.policies,
          overlap: ScheduleOverlapPolicy.SKIP,
        },
        action: scheduleAction(),
      }))
    } else {
      throw err
    }
  }
}
