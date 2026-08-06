import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../service'
import { ingestSecurityContacts } from '../workflows'

// Unlike workflowRunTimeout, workflowExecutionTimeout bounds the entire continueAsNew chain —
// caps a run to 24h so it can't still be going when the next day's tick fires.
const SCHEDULE_ID = 'security-contacts-ingestion'
const WORKFLOW_EXECUTION_TIMEOUT = '24 hours'

function scheduleAction() {
  return {
    type: 'startWorkflow' as const,
    workflowType: ingestSecurityContacts,
    workflowId: 'security-contacts-daily',
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

export async function scheduleSecurityContactsIngestion(): Promise<void> {
  const { temporal } = svc
  if (!temporal) throw new Error('Temporal client not initialized')

  try {
    await temporal.schedule.create({
      scheduleId: SCHEDULE_ID,
      spec: {
        cronExpressions: ['0 6 * * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 hour',
      },
      action: scheduleAction(),
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      // create() only runs once; reconcile on every startup so action/policy changes reach
      // the already-existing live schedule.
      svc.log.info('Schedule security-contacts-ingestion already exists, reconciling action.')
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
