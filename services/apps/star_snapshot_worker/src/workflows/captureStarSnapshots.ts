import {
  ApplicationFailure,
  continueAsNew,
  log,
  proxyActivities,
  sleep,
  workflowInfo,
} from '@temporalio/workflow'

import * as activities from '../activities'

const { findReposForStarSnapshot, fetchAndSaveStarSnapshotBatch } = proxyActivities<
  typeof activities
>({
  startToCloseTimeout: '2 minutes',
  retry: { maximumAttempts: 3, backoffCoefficient: 2 },
})

const GRAPHQL_BATCH_SIZE = 100
const CONCURRENCY = 5
const PAGE_SIZE = 2_000

export interface ICaptureStarSnapshotsArgs {
  capturedAt?: string
  afterUrl?: string
  totalSoFar?: number
  succeededSoFar?: number
  failedSoFar?: number
}

export async function captureStarSnapshots(args: ICaptureStarSnapshotsArgs = {}): Promise<void> {
  const capturedAt = args.capturedAt ?? workflowInfo().startTime.toISOString()
  const repos = await findReposForStarSnapshot(PAGE_SIZE, args.afterUrl)

  const batches: (typeof repos)[] = []
  for (let i = 0; i < repos.length; i += GRAPHQL_BATCH_SIZE) {
    batches.push(repos.slice(i, i + GRAPHQL_BATCH_SIZE))
  }

  let succeeded = args.succeededSoFar ?? 0
  let failed = args.failedSoFar ?? 0
  let rejectedBatches = 0

  for (let i = 0; i < batches.length; i += CONCURRENCY) {
    // Rate-limited batches retry in place via a durable workflow sleep, not a blocking
    // activity call - mirrors backfillStarHistoryBatch's handling of the same quota problem.
    let window = batches.slice(i, i + CONCURRENCY)

    while (window.length > 0) {
      const results = await Promise.allSettled(
        window.map((batch) => fetchAndSaveStarSnapshotBatch(batch, capturedAt)),
      )

      const stillPending: (typeof batches)[number][] = []
      let waitMs = 0

      for (const [idx, result] of results.entries()) {
        if (result.status === 'rejected') {
          rejectedBatches++
          failed += window[idx].length
          log.warn('Failed to capture star snapshot batch', {
            repoCount: window[idx].length,
            error: (result.reason as Error)?.message ?? result.reason,
          })
          continue
        }

        if (result.value.outcome === 'rate-limited') {
          stillPending.push(window[idx])
          waitMs = Math.max(waitMs, result.value.waitMs)
          continue
        }

        for (const repoResult of result.value.results) {
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

      window = stillPending
      if (window.length > 0) {
        log.warn('star snapshot capture GraphQL rate limit near reserved floor, backing off', {
          waitMs,
          batchesWaiting: window.length,
        })
        await sleep(waitMs)
      }
    }
  }

  const total = (args.totalSoFar ?? 0) + repos.length

  // A few rejected batches self-heal (next run's diff, or the gap backfill) - only a fully
  // wiped-out page signals something systemic (auth/token/outage) worth failing the run over.
  if (batches.length > 0 && rejectedBatches === batches.length) {
    // A plain Error only fails the workflow task (infinite replay); ApplicationFailure
    // is required to fail the execution so the schedule's retry policy engages.
    throw ApplicationFailure.create({
      message: `all ${batches.length} batch(es) failed after retries on this page; ${succeeded} succeeded and ${failed} failed so far (already-persisted snapshots are safe to retry)`,
      type: 'StarSnapshotBatchFailure',
    })
  }

  if (rejectedBatches > 0) {
    log.error('star snapshot capture had partial batch failures on this page', {
      rejectedBatches,
      totalBatches: batches.length,
      succeeded,
      failed,
    })
  }

  if (repos.length === PAGE_SIZE) {
    await continueAsNew<typeof captureStarSnapshots>({
      capturedAt,
      afterUrl: repos[repos.length - 1].repoUrl,
      totalSoFar: total,
      succeededSoFar: succeeded,
      failedSoFar: failed,
    })
    return
  }

  log.info('captureStarSnapshots complete', { total, succeeded, failed })
}
