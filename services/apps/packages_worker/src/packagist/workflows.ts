import {
  ParentClosePolicy,
  WorkflowIdReusePolicy,
  continueAsNew,
  log,
  proxyActivities,
  startChild,
} from '@temporalio/workflow'

import type * as activities from './activities'
import { INGEST_MAX_ATTEMPTS } from './retryPolicy'

const acts = proxyActivities<typeof activities>({
  startToCloseTimeout: '15 minutes',
  retry: {
    initialInterval: '30 seconds',
    backoffCoefficient: 2,
    maximumAttempts: INGEST_MAX_ATTEMPTS,
  },
})

const INGEST_BATCH = 50
const ROUNDS_PER_RUN = 20

interface MetadataState {
  cutoff?: string
  cursor?: string
}

interface DownloadsState {
  cutoff?: string
  cursor?: string
}

export async function seedPackagistPackages(): Promise<void> {
  await acts.runPackagistPackageSeed()

  // Chain the drain off seed completion (not a cron) so newly discovered packages exist
  // as rows first. ABANDON so it outlives this workflow; fixed id + ALLOW_DUPLICATE means
  // a drain that outlasts the week makes next Sunday's seed skip its start instead of
  // doubling the crawl (a still-RUNNING id always throws regardless of reuse policy).
  try {
    await startChild(ingestPackagistMetadata, {
      workflowId: 'packagist-metadata-drain',
      workflowIdReusePolicy: WorkflowIdReusePolicy.ALLOW_DUPLICATE,
      args: [{}],
      parentClosePolicy: ParentClosePolicy.ABANDON,
    })
  } catch (err) {
    if (err instanceof Error && err.name === 'WorkflowExecutionAlreadyStartedError') {
      log.warn('packagist metadata drain still running from a prior seed — skipping chain-start')
      return
    }
    throw err
  }
}

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
    if (candidates.length === 0) return
    await acts.ingestPackagistMetadataBatch(candidates)
    cursor = nextCursor
    if (stopAfterFirstPage) return
    if (candidates.length < INGEST_BATCH) return
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
