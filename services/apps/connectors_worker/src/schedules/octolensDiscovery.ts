import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../main'
import { octolensDiscovery } from '../workflows/octolensDiscovery'

// SKIP overlap is required: concurrent runs could soft-delete each other's new integrations.
export async function scheduleOctolensDiscovery(): Promise<void> {
  try {
    await svc.temporal.schedule.create({
      scheduleId: 'connectors-octolens-discovery',
      spec: {
        intervals: [{ every: '1 hour' }],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 hour',
      },
      action: {
        type: 'startWorkflow',
        workflowType: octolensDiscovery,
        taskQueue: 'connectors',
        args: [],
        workflowExecutionTimeout: '30 minutes',
      },
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info('Octolens discovery schedule already registered in Temporal.')
    } else {
      throw new Error(err)
    }
  }
}
