import {
  ParentClosePolicy,
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

// Fans out zero-row repos to abandoned child workflows instead of processing them
// in-line - each batch keeps its own rate-limit retry loop, so one page of candidates
// isn't held up waiting on GitHub's reset for another.
export async function selfHealStarBackfill(args: ISelfHealStarBackfillArgs = {}): Promise<void> {
  const repos = await findReposNeedingStarBackfill(PAGE_SIZE, args.afterUrl)
  let batchesDispatched = args.batchesDispatchedSoFar ?? 0

  for (let i = 0; i < repos.length; i += BATCH_SIZE) {
    const batch = repos.slice(i, i + BATCH_SIZE)
    await startChild(backfillStarHistoryBatch, {
      workflowId: `${workflowInfo().workflowId}/batch-${batchesDispatched}`,
      parentClosePolicy: ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON,
      args: [{ repos: batch }],
    })
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
