import {
  ApplicationFailure,
  continueAsNew,
  log,
  patched,
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
// A rejected batch usually means a transient blip - one retry pass after a cooldown
// recovers most of them instead of leaving a permanent gap for that day.
const REJECTED_BATCH_RETRY_DELAY_MS = 30_000

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
  let rejectedBatches: (typeof batches)[number][] = []

  // Runs one window of batches to completion, handling rate-limit backoff in place.
  // Returns the batches that were still rejected (activity retries exhausted) when done.
  const runWindow = async (
    initialWindow: (typeof batches)[number][],
  ): Promise<(typeof batches)[number][]> => {
    let window = initialWindow
    const rejected: (typeof batches)[number][] = []

    while (window.length > 0) {
      const results = await Promise.allSettled(
        window.map((batch) => fetchAndSaveStarSnapshotBatch(batch, capturedAt)),
      )

      const stillPending: (typeof batches)[number][] = []
      let waitMs = 0

      for (const [idx, result] of results.entries()) {
        if (result.status === 'rejected') {
          rejected.push(window[idx])
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

    return rejected
  }

  for (let i = 0; i < batches.length; i += CONCURRENCY) {
    rejectedBatches.push(...(await runWindow(batches.slice(i, i + CONCURRENCY))))
  }

  // One retry pass over whatever's still rejected before giving up on it, even if every
  // batch on this page rejected (small/last page - still worth one cooldown retry) (CM-1441).
  // Gated by patched() - an execution already in flight when this shipped must keep replaying
  // its old command sequence (straight to the page decision below) or it'll hit a nondeterminism
  // error; only executions that start fresh after the deploy take the new retry-then-decide path.
  if (rejectedBatches.length > 0 && patched('CM-1441-retry-rejected-batches')) {
    await sleep(REJECTED_BATCH_RETRY_DELAY_MS)
    const stillRejected: (typeof batches)[number][] = []
    for (let i = 0; i < rejectedBatches.length; i += CONCURRENCY) {
      stillRejected.push(...(await runWindow(rejectedBatches.slice(i, i + CONCURRENCY))))
    }
    for (const batch of stillRejected) {
      failed += batch.length
    }
    rejectedBatches = stillRejected
  }

  const total = (args.totalSoFar ?? 0) + repos.length

  // A few rejected batches self-heal (next run's diff, or the gap backfill) - only a fully
  // wiped-out page signals something systemic (auth/token/outage) worth failing the run over.
  if (batches.length > 0 && rejectedBatches.length === batches.length) {
    // A plain Error only fails the workflow task (infinite replay); ApplicationFailure
    // is required to fail the execution so the schedule's retry policy engages.
    throw ApplicationFailure.create({
      message: `all ${batches.length} batch(es) failed after retries on this page; ${succeeded} succeeded and ${failed} failed so far (already-persisted snapshots are safe to retry)`,
      type: 'StarSnapshotBatchFailure',
    })
  }

  if (rejectedBatches.length > 0) {
    log.error('star snapshot capture had partial batch failures on this page', {
      rejectedBatches: rejectedBatches.length,
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
