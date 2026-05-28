import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../service'
import { osvSync } from '../workflows'

const SCHEDULE_ID = 'osv-advisories-sync'

function getEcosystems(): string[] {
  const raw = process.env.OSV_ECOSYSTEMS
  if (!raw) throw new Error('Missing required environment variable: OSV_ECOSYSTEMS')
  const list = raw
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)
  if (list.length === 0) throw new Error('OSV_ECOSYSTEMS resolved to an empty list')
  return list
}

// Registers the daily OSV advisory sync schedule if it doesn't already exist.
// If the schedule already exists in Temporal, we log and leave it unchanged (no update).
// Cron is offset from npm-registry-ingest (`15 3 * * *`) so the two large daily ingest jobs don't fight for the same DB at the same minute.
export async function scheduleOsvSync(): Promise<void> {
  const { temporal } = svc
  if (!temporal) throw new Error('Temporal client not initialized')

  try {
    await temporal.schedule.create({
      scheduleId: SCHEDULE_ID,
      spec: {
        cronExpressions: ['30 3 * * *'],
      },
      policies: {
        // SKIP per ADR-0001 §Worker architecture: if a slow run is still
        // executing when the next fire time comes around, don't queue a
        // second concurrent run.
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 hour',
      },
      action: {
        type: 'startWorkflow',
        workflowType: osvSync,
        taskQueue: 'packages-worker',
        // Headroom for npm (~1 hour today) + Maven (~5 minutes) + derive
        // (~5 minutes for 600-700k packages); 4 hours leaves space for the
        // upsertOne N+1 deferred fix being slower than expected.
        workflowExecutionTimeout: '4 hours',
        retry: {
          initialInterval: '30 seconds',
          backoffCoefficient: 2,
          maximumAttempts: 3,
        },
        args: [{ ecosystems: getEcosystems() }],
      },
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info(`Schedule ${SCHEDULE_ID} already registered.`)
    } else {
      throw err
    }
  }
}
