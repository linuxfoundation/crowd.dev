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

// A retry of this workflow (the schedule's `retry` policy) restarts with fresh args, so it
// re-derives the same batch-N workflow IDs already dispatched by the failed attempt. Those
// children run ABANDONED, so they're either still running or already finished - either way,
// a start collision here means "already handled," not a real failure. REJECT_DUPLICATE is
// required for that: the default ALLOW_DUPLICATE lets a new execution silently replace a
// already-completed one with the same ID instead of throwing, which would re-run the whole
// batch (re-fetching every repo's stargazer history) after its in-flight claims were released.
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

// Fans out zero-row repos to abandoned child workflows instead of processing them
// in-line - each batch keeps its own rate-limit retry loop, so one page of candidates
// isn't held up waiting on GitHub's reset for another.
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
