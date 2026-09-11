import { log, proxyActivities, workflowInfo } from '@temporalio/workflow'

import * as activities from '../activities'

const { findReposForStarSnapshot, fetchAndSaveStarSnapshotBatch } = proxyActivities<
  typeof activities
>({
  startToCloseTimeout: '2 minutes',
  retry: { maximumAttempts: 3, backoffCoefficient: 2 },
})

const GRAPHQL_BATCH_SIZE = 100
const CONCURRENCY = 5

export async function captureStarSnapshots(): Promise<void> {
  const capturedAt = workflowInfo().startTime.toISOString()
  const repos = await findReposForStarSnapshot()

  const batches: (typeof repos)[] = []
  for (let i = 0; i < repos.length; i += GRAPHQL_BATCH_SIZE) {
    batches.push(repos.slice(i, i + GRAPHQL_BATCH_SIZE))
  }

  let succeeded = 0
  let failed = 0

  for (let i = 0; i < batches.length; i += CONCURRENCY) {
    const window = batches.slice(i, i + CONCURRENCY)
    const results = await Promise.allSettled(
      window.map((batch) => fetchAndSaveStarSnapshotBatch(batch, capturedAt)),
    )

    for (const [idx, result] of results.entries()) {
      if (result.status === 'rejected') {
        failed += window[idx].length
        log.warn('Failed to capture star snapshot batch', {
          repoCount: window[idx].length,
          error: (result.reason as Error)?.message ?? result.reason,
        })
        continue
      }

      for (const repoResult of result.value) {
        if (repoResult.error) {
          failed++
          log.warn('Failed to capture star snapshot', {
            repoUrl: repoResult.repoUrl,
            error: repoResult.error,
          })
        } else {
          succeeded++
        }
      }
    }
  }

  log.info('captureStarSnapshots complete', { total: repos.length, succeeded, failed })
}
