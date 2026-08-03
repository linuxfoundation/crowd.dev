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

// Prepare runs one long DB statement (a full package_dependencies scan) — long timeout
// plus a timer heartbeat so a lost worker surfaces in minutes. The short merge/finish
// steps ride the default ingest proxy above.
const transitivePrepareActs = proxyActivities<typeof activities>({
  startToCloseTimeout: '45 minutes',
  heartbeatTimeout: '2 minutes',
  retry: {
    initialInterval: '1 minute',
    backoffCoefficient: 2,
    // Lockstep with the activity's terminal fail-marking — see retryPolicy.ts.
    maximumAttempts: TRANSITIVE_PREPARE_MAX_ATTEMPTS,
  },
})

interface TransitiveState {
  runId?: number
  cursor?: string
  processed?: number
  changed?: number
}

// ActivityFailure's own message is the generic "Activity task failed" — the reason a
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

// Prepare (run row + edge snapshot + closure) once, then drain the keyset merge in
// continueAsNew rounds; resumed generations carry the run id and skip prepare.
export async function computePackagistTransitiveDependents(
  state: TransitiveState = {},
): Promise<void> {
  const runId =
    state.runId ?? (await transitivePrepareActs.preparePackagistTransitiveCounts()).runId
  let cursor = state.cursor ?? ''
  let processed = state.processed ?? 0
  let changed = state.changed ?? 0

  // Prepare marks its own failures; this catch is the merge phase's terminal marking,
  // so a permanently failed drain never sits in 'merging' forever. continueAsNew stays
  // outside the try — it must never be treated as a failure.
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
    await acts.failPackagistTransitiveRun(runId, rootErrorMessage(err))
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

// Chain-start a drain as an abandoned child with a fixed workflow id: ABANDON lets it
// outlive the parent; ALLOW_DUPLICATE + the catch mean a drain still running from a
// prior cycle skips this start instead of doubling the work (a still-RUNNING id always
// throws regardless of reuse policy).
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

// Edges refresh with the weekly metadata drain, so the transitive closure chains off its
// completion — an event, not a clock offset.
const chainTransitiveDrain = (): Promise<void> =>
  chainDrain(
    computePackagistTransitiveDependents,
    'packagist-transitive-drain',
    'packagist transitive drain still running — skipping chain-start',
  )

// The cutoff is fixed once per run (deterministic activity), same pattern as the
// downloads-30d/daily lanes — a keyset scan only ever visits each purl once per drain,
// so due-selection must be anchored to a stable point in time rather than a live NOW()
// that would let a purl processed early in the run dodge this cycle's refresh window.
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
      // A debug-bounded run must stay bounded — never kick off the closure's full
      // package_dependencies scan from it, even when the queue happens to be empty.
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
