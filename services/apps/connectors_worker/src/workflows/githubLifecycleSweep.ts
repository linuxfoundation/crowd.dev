import {
  ParentClosePolicy,
  executeChild,
  isCancellation,
  log,
  proxyActivities,
} from '@temporalio/workflow'

import type * as activities from '../activities/githubLifecycleActivities'
import {
  LIFECYCLE_SWEEP_CONCURRENCY,
  reconcileGithubIntegrationWorkflowId,
} from '../githubLifecycle'
import { chunk } from '../mirrorSweepLimits'
import { reconcileGithubIntegration } from './reconcileGithubIntegration'

const ALREADY_STARTED_ERROR = 'WorkflowExecutionAlreadyStartedError'

const activity = proxyActivities<typeof activities>({
  startToCloseTimeout: '2 minutes',
  retry: {
    maximumAttempts: 3,
    backoffCoefficient: 2,
  },
})

export interface IGithubLifecycleSweepSummary {
  integrations: number
  reconciled: number
  skipped: number
  failed: number
}

type ReconcileOutcome = 'reconciled' | 'skipped' | 'failed'

export async function githubLifecycleSweep(): Promise<IGithubLifecycleSweepSummary> {
  const integrationIds = await activity.listIntegrationsNeedingReconcile()
  const summary: IGithubLifecycleSweepSummary = {
    integrations: integrationIds.length,
    reconciled: 0,
    skipped: 0,
    failed: 0,
  }

  for (const ids of chunk(integrationIds, LIFECYCLE_SWEEP_CONCURRENCY)) {
    const outcomes = await Promise.all(ids.map(reconcileIntegration))
    for (const outcome of outcomes) {
      summary[outcome] += 1
    }
  }

  log.info('github lifecycle sweep complete', { ...summary })
  return summary
}

async function reconcileIntegration(integrationId: string): Promise<ReconcileOutcome> {
  try {
    await executeChild(reconcileGithubIntegration, {
      workflowId: reconcileGithubIntegrationWorkflowId(integrationId),
      args: [{ integrationId }],
      parentClosePolicy: ParentClosePolicy.ABANDON,
      workflowExecutionTimeout: '4 hours',
    })
    return 'reconciled'
  } catch (err) {
    if (isCancellation(err)) {
      throw err
    }
    if (err instanceof Error && err.name === ALREADY_STARTED_ERROR) {
      return 'skipped'
    }
    log.error('github lifecycle sweep child failed', {
      integrationId,
      error: err instanceof Error ? err.message : String(err),
    })
    return 'failed'
  }
}
