import { log, proxyActivities, sleep, workflowInfo } from '@temporalio/workflow'

import { IRepoForStarSnapshot } from '@crowd/types'

import * as activities from '../activities'

const { backfillRepoStarHistory } = proxyActivities<typeof activities>({
  // No heartbeats - a large/old repo pages its full stargazer history then writes one row per
  // historical day in one call; 10 minutes covers that without false-dead-lettering an old repo.
  startToCloseTimeout: '10 minutes',
  retry: { maximumAttempts: 3, backoffCoefficient: 2 },
})

export interface IBackfillStarHistoryBatchArgs {
  repos: IRepoForStarSnapshot[]
}

// Rate-limited repos retry in place via a durable workflow sleep, not a blocking activity
// call, so it's safe even when GitHub's reset is the better part of an hour away.
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
        // A rejection means every retry of the activity itself failed to run (e.g. a DB blip) -
        // the repo is left neither recorded as failed nor retried within this batch.
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
