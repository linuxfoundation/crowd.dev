import {
  ParentClosePolicy,
  WorkflowIdReusePolicy,
  continueAsNew,
  log,
  proxyActivities,
  startChild,
} from '@temporalio/workflow'

import type * as activities from './activities'
import { INGEST_MAX_ATTEMPTS, TRANSITIVE_PREPARE_MAX_ATTEMPTS } from './retryPolicy'

const acts = proxyActivities<typeof activities>({
  startToCloseTimeout: '15 minutes',
  retry: {
    initialInterval: '30 seconds',
    backoffCoefficient: 2,
    maximumAttempts: INGEST_MAX_ATTEMPTS,
  },
})

const INGEST_BATCH = 50
export const ROUNDS_PER_RUN = 20

interface MetadataState {
  cutoff?: string
  cursor?: string
}

export const TRANSITIVE_MERGE_BATCH = 10_000

const transitivePrepareActs = proxyActivities<typeof activities>({
  startToCloseTimeout: '90 minutes',
  heartbeatTimeout: '2 minutes',
  retry: {
    initialInterval: '1 minute',
    backoffCoefficient: 2,
    // Lockstep with the activity's terminal fail-marking (see retryPolicy.ts).
    maximumAttempts: TRANSITIVE_PREPARE_MAX_ATTEMPTS,
  },
})

interface TransitiveState {
  runId?: number
  cursor?: string
  processed?: number
  changed?: number
}

// ActivityFailure's own message is the generic "Activity task failed"; the reason a
// human wants in error_message sits at the bottom of the cause chain.
function rootErrorMessage(err: unknown): string {
  let cur = err
  for (;;) {
    // `cause` is untyped under the es2017 lib this workspace compiles with, but it is
    // present at runtime on Node 20 / Temporal failures.
    const cause = cur instanceof Error ? (cur as { cause?: unknown }).cause : undefined
    if (!(cause instanceof Error)) break
    cur = cause
  }
  return cur instanceof Error ? cur.message : String(cur)
}

export async function computePackagistTransitiveDependents(
  state: TransitiveState = {},
): Promise<void> {
  const runId =
    state.runId ?? (await transitivePrepareActs.preparePackagistTransitiveCounts()).runId
  let cursor = state.cursor ?? ''
  let processed = state.processed ?? 0
  let changed = state.changed ?? 0

  try {
    for (let r = 0; r < ROUNDS_PER_RUN; r++) {
      const batch = await acts.mergePackagistTransitiveBatch(cursor, TRANSITIVE_MERGE_BATCH)
      processed += batch.processed
      changed += batch.changed
      if (batch.processed < TRANSITIVE_MERGE_BATCH) {
        await acts.finishPackagistTransitiveRun(runId, { processed, changed })
        return
      }
      cursor = batch.nextCursor
    }
  } catch (err) {
    // Best-effort: fail-marking must never replace the drain's original error.
    try {
      await acts.failPackagistTransitiveRun(runId, rootErrorMessage(err))
    } catch (markErr) {
      log.warn(`could not fail-mark transitive run ${runId}: ${String(markErr)}`)
    }
    throw err
  }

  await continueAsNew<typeof computePackagistTransitiveDependents>({
    runId,
    cursor,
    processed,
    changed,
  })
}

interface DownloadsState {
  cutoff?: string
  cursor?: string
}

async function chainDrain(
  workflow: typeof ingestPackagistMetadata | typeof computePackagistTransitiveDependents,
  workflowId: string,
  stillRunningMessage: string,
): Promise<void> {
  try {
    await startChild(workflow, {
      workflowId,
      workflowIdReusePolicy: WorkflowIdReusePolicy.ALLOW_DUPLICATE,
      args: [{}],
      parentClosePolicy: ParentClosePolicy.ABANDON,
    })
  } catch (err) {
    if (err instanceof Error && err.name === 'WorkflowExecutionAlreadyStartedError') {
      log.warn(stillRunningMessage)
      return
    }
    throw err
  }
}

export async function seedPackagistPackages(): Promise<void> {
  await acts.runPackagistPackageSeed()

  // Chain the drain off seed completion (not a cron) so newly discovered packages exist
  // as rows first.
  await chainDrain(
    ingestPackagistMetadata,
    'packagist-metadata-drain',
    'packagist metadata drain still running from a prior seed — skipping chain-start',
  )
}

const chainTransitiveDrain = (): Promise<void> =>
  chainDrain(
    computePackagistTransitiveDependents,
    'packagist-transitive-drain',
    'packagist transitive drain still running — skipping chain-start',
  )

const TRANSITIVE_BACKSTOP_FRESH_DAYS = 6

// Clock-based safety net for the event chain: a broken seed or metadata drain means no
// chain fired this week, so start the closure anyway instead of letting counts go stale.
// Ledger-gated (a healthy week costs no second scan) and routed through the fixed
// workflow id, so it can never race a live drain.
export async function backstopPackagistTransitiveDrain(): Promise<void> {
  if (await acts.packagistTransitiveRanRecently(TRANSITIVE_BACKSTOP_FRESH_DAYS)) return
  // A mid-crawl metadata drain will chain the closure itself on completion; starting it
  // now would snapshot changing edges AND make that completion chain-start bounce.
  if (await acts.packagistMetadataDrainRunning()) return
  await chainTransitiveDrain()
}

export async function ingestPackagistMetadata(state: MetadataState = {}): Promise<void> {
  const cutoff = state.cutoff ?? (await acts.packagistCurrentTimestamp())
  let cursor = state.cursor || ''
  const stopAfterFirstPage = await acts.packagistStopAfterFirstPage()

  for (let r = 0; r < ROUNDS_PER_RUN; r++) {
    const { candidates, nextCursor } = await acts.getPackagistMetadataBatch(
      cutoff,
      cursor,
      INGEST_BATCH,
    )
    if (candidates.length === 0) {
      if (!stopAfterFirstPage) await chainTransitiveDrain()
      return
    }
    await acts.ingestPackagistMetadataBatch(candidates)
    cursor = nextCursor
    if (stopAfterFirstPage) return
    if (candidates.length < INGEST_BATCH) {
      await chainTransitiveDrain()
      return
    }
  }

  await continueAsNew<typeof ingestPackagistMetadata>({ cutoff, cursor })
}

// Monthly capture of the observed rolling 30d window for every packagist package.
// The cutoff is fixed once per run (deterministic activity) so the watermark-based
// due-selection drains the whole universe exactly once per cron fire.
export async function ingestPackagistDownloads30d(state: DownloadsState = {}): Promise<void> {
  const cutoff = state.cutoff ?? (await acts.packagistCurrentTimestamp())
  // Packagist's monthly window is labeled by calendar month (see downloads.ts), so the
  // write-date must come from the run's fixed cutoff — a drain that runs past real UTC
  // midnight on the 1st must not let later batches slide into the next month's window.
  const runDate = cutoff.slice(0, 10)
  let cursor = state.cursor || ''
  const stopAfterFirstPage = await acts.packagistStopAfterFirstPage()

  for (let r = 0; r < ROUNDS_PER_RUN; r++) {
    const { purls, nextCursor } = await acts.getPackagist30dBatch(cutoff, cursor, INGEST_BATCH)
    if (purls.length === 0) return
    await acts.ingestPackagist30dBatch(purls, runDate)
    cursor = nextCursor
    if (stopAfterFirstPage) return
    if (purls.length < INGEST_BATCH) return
  }

  await continueAsNew<typeof ingestPackagistDownloads30d>({ cutoff, cursor })
}

// Daily downloads capture for the critical slice.
export async function ingestPackagistDownloadsDaily(state: DownloadsState = {}): Promise<void> {
  if ((await acts.getCriticalPackagistCount()) === 0) return

  const cutoff = state.cutoff ?? (await acts.packagistCurrentTimestamp())
  // Packagist's `daily` figure is tied to a specific calendar day (see schedule.ts) —
  // derive the write-date from the run's fixed cutoff instead of re-reading the clock
  // per batch, so a drain that runs past UTC midnight still tags every row consistently.
  const runDate = cutoff.slice(0, 10)
  let cursor = state.cursor || ''
  const stopAfterFirstPage = await acts.packagistStopAfterFirstPage()

  for (let r = 0; r < ROUNDS_PER_RUN; r++) {
    const { candidates, nextCursor } = await acts.getPackagistDailyBatch(
      cutoff,
      cursor,
      INGEST_BATCH,
    )
    if (candidates.length === 0) return
    await acts.ingestPackagistDailyBatch(candidates, runDate)
    cursor = nextCursor
    if (stopAfterFirstPage) return
    if (candidates.length < INGEST_BATCH) return
  }

  await continueAsNew<typeof ingestPackagistDownloadsDaily>({ cutoff, cursor })
}
