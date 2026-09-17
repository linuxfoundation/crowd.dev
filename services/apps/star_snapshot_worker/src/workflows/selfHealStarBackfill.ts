import {
  ParentClosePolicy,
  WorkflowIdReusePolicy,
  continueAsNew,
  log,
  proxyActivities,
  startChild,
  workflowInfo,
} from '@temporalio/workflow'

import * as activities from '../activities'

import { backfillStarHistoryBatch } from './backfillStarHistoryBatch'

const { findReposNeedingStarBackfill } = proxyActivities<typeof activities>({
  startToCloseTimeout: '2 minutes',
  retry: { maximumAttempts: 3, backoffCoefficient: 2 },
})

const PAGE_SIZE = 1_000
const BATCH_SIZE = 100

export interface ISelfHealStarBackfillArgs {
  afterUrl?: string
  batchesDispatchedSoFar?: number
}

// A workflow retry re-derives the same batch-N IDs; REJECT_DUPLICATE is required so that
// collision throws instead of ALLOW_DUPLICATE silently re-running an already-finished batch.
async function startBatchChild(
  batch: Awaited<ReturnType<typeof findReposNeedingStarBackfill>>,
  workflowId: string,
): Promise<void> {
  try {
    await startChild(backfillStarHistoryBatch, {
      workflowId,
      workflowIdReusePolicy: WorkflowIdReusePolicy.REJECT_DUPLICATE,
      parentClosePolicy: ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON,
      args: [{ repos: batch }],
    })
  } catch (err) {
    if (!(err instanceof Error) || err.name !== 'WorkflowExecutionAlreadyStartedError') {
      throw err
    }
    log.warn(
      'batch child already started for this id, skipping (likely a retry of this workflow)',
      {
        workflowId,
      },
    )
  }
}

// Fans out to abandoned child workflows so each batch's own rate-limit retry loop doesn't
// hold up other pages waiting on GitHub's reset.
export async function selfHealStarBackfill(args: ISelfHealStarBackfillArgs = {}): Promise<void> {
  const repos = await findReposNeedingStarBackfill(PAGE_SIZE, args.afterUrl)
  let batchesDispatched = args.batchesDispatchedSoFar ?? 0

  for (let i = 0; i < repos.length; i += BATCH_SIZE) {
    const batch = repos.slice(i, i + BATCH_SIZE)
    await startBatchChild(batch, `${workflowInfo().workflowId}/batch-${batchesDispatched}`)
    batchesDispatched++
  }

  if (repos.length === PAGE_SIZE) {
    await continueAsNew<typeof selfHealStarBackfill>({
      afterUrl: repos[repos.length - 1].repoUrl,
      batchesDispatchedSoFar: batchesDispatched,
    })
    return
  }

  log.info('selfHealStarBackfill complete', { batchesDispatched })
}
