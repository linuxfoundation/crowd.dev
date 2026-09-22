import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../../service'
import { ingestReportingProtocols } from '../../workflows'

const SCHEDULE_ID = 'reporting-protocol-ingestion'
const WORKFLOW_EXECUTION_TIMEOUT = '24 hours'

function scheduleAction() {
  return {
    type: 'startWorkflow' as const,
    workflowType: ingestReportingProtocols,
    workflowId: 'reporting-protocol-daily',
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

export async function scheduleReportingProtocolIngestion(): Promise<void> {
  const { temporal } = svc
  if (!temporal) throw new Error('Temporal client not initialized')

  try {
    await temporal.schedule.create({
      scheduleId: SCHEDULE_ID,
      spec: {
        cronExpressions: ['0 7 * * *'],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 hour',
      },
      action: scheduleAction(),
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info('Schedule reporting-protocol-ingestion already exists, reconciling action.')
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
