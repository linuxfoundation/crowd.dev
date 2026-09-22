import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../service'
import { osvSync } from '../workflows'

const SCHEDULE_ID = 'osv-advisories-sync'

// Ecosystems we support today. The value here doubles as the case-sensitive
// path segment in OSV's bucket URL (<base>/<ecosystem>/all.zip), so a typo
// like `OSV_ECOSYSTEMS=maven` (lowercase) would 404 silently every day. We
// validate the env input against this list and refuse to register the
// schedule on a mismatch — better a loud startup error than a silent miss.
// Add new entries here when v1 expands beyond npm + Maven.
const VALID_ECOSYSTEMS = ['npm', 'Maven', 'cargo', 'NuGet', 'RubyGems', 'Go'] as const

function getEcosystems(): string[] {
  const raw = process.env.OSV_ECOSYSTEMS
  if (!raw) throw new Error('Missing required environment variable: OSV_ECOSYSTEMS')
  const list = raw
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)
  const deduped = [...new Set(list)]
  if (deduped.length === 0) throw new Error('OSV_ECOSYSTEMS resolved to an empty list')
  for (const eco of deduped) {
    if (!(VALID_ECOSYSTEMS as readonly string[]).includes(eco)) {
      const hint = VALID_ECOSYSTEMS.find((v) => v.toLowerCase() === eco.toLowerCase())
      const supported = VALID_ECOSYSTEMS.join(', ')
      const msg = hint
        ? `OSV_ECOSYSTEMS contains "${eco}" — did you mean "${hint}"? OSV's bucket paths are case-sensitive.`
        : `OSV_ECOSYSTEMS contains unsupported ecosystem "${eco}". Supported: ${supported}.`
      throw new Error(msg)
    }
  }
  return deduped
}

function scheduleAction() {
  return {
    type: 'startWorkflow' as const,
    workflowType: osvSync,
    taskQueue: 'osv-worker',
    // Headroom for npm (~1 hour today) + Maven (~5 minutes) + derive
    // (~5 minutes for 600-700k packages); 4 hours leaves space for the
    // upsertOne N+1 deferred fix being slower than expected.
    workflowExecutionTimeout: '4 hours',
    retry: {
      initialInterval: '30 seconds',
      backoffCoefficient: 2,
      maximumAttempts: 3,
    },
    args: [{ ecosystems: getEcosystems() }] as [{ ecosystems: string[] }],
  }
}

// Registers the daily OSV advisory sync schedule if it doesn't already exist.
// If the schedule already exists in Temporal, we reconcile its action so a
// changed OSV_ECOSYSTEMS env var reaches an already-running schedule on restart.
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
