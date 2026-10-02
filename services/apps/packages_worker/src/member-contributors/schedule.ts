import { ScheduleAlreadyRunning, ScheduleOverlapPolicy } from '@temporalio/client'

import { svc } from '../service'
import { syncGovernanceFileContributors, syncRepoContributorsFromGitActivity } from '../workflows'
import { GitActivitySyncOptions } from './git-activity/syncGitActivityContributors'

const TASK_QUEUE = 'security-contacts-worker'
const RETRY = {
  initialInterval: '30 seconds',
  backoffCoefficient: 2,
  maximumAttempts: 3,
}

interface ContributorsSchedule {
  scheduleId: string
  cron: string
  action: {
    type: 'startWorkflow'
    workflowType: typeof syncGovernanceFileContributors | typeof syncRepoContributorsFromGitActivity
    workflowId: string
    taskQueue: string
    workflowExecutionTimeout: string
    retry: typeof RETRY
    args: [] | [GitActivitySyncOptions]
  }
}

function governanceFileSchedule(): ContributorsSchedule {
  return {
    scheduleId: 'governance-file-contributors-sync',
    cron: '0 5 * * *',
    action: {
      type: 'startWorkflow',
      workflowType: syncGovernanceFileContributors,
      workflowId: 'governance-file-contributors-daily',
      taskQueue: TASK_QUEUE,
      workflowExecutionTimeout: '6 hours',
      retry: RETRY,
      args: [],
    },
  }
}

function gitActivitySchedule(full: boolean): ContributorsSchedule {
  const mode = full ? 'full' : 'incremental'
  return {
    scheduleId: `git-activity-contributors-${mode}-sync`,
    cron: full ? '0 6 1,15 * *' : '0 6 2-14,16-31 * *',
    action: {
      type: 'startWorkflow',
      workflowType: syncRepoContributorsFromGitActivity,
      workflowId: `git-activity-contributors-${mode}`,
      taskQueue: TASK_QUEUE,
      workflowExecutionTimeout: '8 hours',
      retry: RETRY,
      args: [{ full }],
    },
  }
}

async function createOrReconcileSchedule(schedule: ContributorsSchedule): Promise<void> {
  const { temporal } = svc
  if (!temporal) throw new Error('Temporal client not initialized')

  try {
    await temporal.schedule.create({
      scheduleId: schedule.scheduleId,
      spec: {
        cronExpressions: [schedule.cron],
      },
      policies: {
        overlap: ScheduleOverlapPolicy.SKIP,
        catchupWindow: '1 hour',
      },
      action: schedule.action,
    })
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      svc.log.info(`Schedule ${schedule.scheduleId} already exists, reconciling action.`)
      const handle = temporal.schedule.getHandle(schedule.scheduleId)
      await handle.update((prev) => ({
        ...prev,
        spec: {
          ...prev.spec,
          cronExpressions: [schedule.cron],
        },
        policies: {
          ...prev.policies,
          overlap: ScheduleOverlapPolicy.SKIP,
        },
        action: schedule.action,
      }))
    } else {
      throw err
    }
  }
}

export async function scheduleGovernanceFileContributorsSync(): Promise<void> {
  await createOrReconcileSchedule(governanceFileSchedule())
}

export async function scheduleGitActivityContributorsSync(): Promise<void> {
  await createOrReconcileSchedule(gitActivitySchedule(false))
  await createOrReconcileSchedule(gitActivitySchedule(true))
}
