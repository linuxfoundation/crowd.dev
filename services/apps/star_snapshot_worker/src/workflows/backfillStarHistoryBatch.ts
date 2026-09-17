import { log, proxyActivities, sleep, workflowInfo } from '@temporalio/workflow'

import { IRepoForStarSnapshot } from '@crowd/types'

import * as activities from '../activities'

const { backfillRepoStarHistory } = proxyActivities<typeof activities>({
  // No heartbeats - a large/old repo pages through its full stargazer history then writes
  // one row per historical day, sequentially, inside a single call. 10 minutes is a generous
  // margin over that under normal conditions, well short of forcing the 3 retries into a
  // false dead-letter for a repo that just happens to be old, not actually stuck.
  startToCloseTimeout: '10 minutes',
  retry: { maximumAttempts: 3, backoffCoefficient: 2 },
})

export interface IBackfillStarHistoryBatchArgs {
  repos: IRepoForStarSnapshot[]
}

// Rate-limited repos are retried in place rather than failing the batch - the wait is a
// durable workflow sleep, not a blocking activity call, so it's safe even when GitHub's
// reset is the better part of an hour away.
export async function backfillStarHistoryBatch(args: IBackfillStarHistoryBatchArgs): Promise<void> {
  let pending = args.repos
  const ownerId = workflowInfo().workflowId

  while (pending.length > 0) {
    const results = await Promise.allSettled(
      pending.map((repo) => backfillRepoStarHistory(repo, ownerId)),
    )

    const stillPending: IRepoForStarSnapshot[] = []
    let waitMs = 0

    results.forEach((result, i) => {
      if (result.status === 'rejected') {
        // The activity already isolates and records every outcome it can reach; a
        // rejection here means every retry attempt itself failed to even run (e.g. a DB
        // blip), so the repo is neither recorded as failed nor retried within this batch.
        log.warn('backfillRepoStarHistory activity exhausted retries', {
          repoUrl: pending[i].repoUrl,
          error: (result.reason as Error)?.message ?? result.reason,
        })
        return
      }
      if (result.value.outcome === 'rate-limited') {
        stillPending.push(pending[i])
        waitMs = Math.max(waitMs, result.value.waitMs)
      } else if (result.value.outcome === 'in-flight') {
        log.debug('repo already claimed by another self-heal batch, skipping', {
          repoUrl: pending[i].repoUrl,
        })
      }
    })

    pending = stillPending
    if (pending.length > 0) {
      await sleep(waitMs)
    }
  }
}
