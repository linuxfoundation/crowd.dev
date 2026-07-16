import { ParentClosePolicy, continueAsNew, proxyActivities, startChild } from '@temporalio/workflow'

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
  cursor?: string
}

interface DownloadsState {
  cutoff?: string
}

export async function seedPackagistPackages(): Promise<void> {
  await acts.runPackagistPackageSeed()

  // Chain the metadata drain off a completed seed instead of a second cron: newly
  // discovered packages are rows before the p2 crawl resolves dependency targets.
  // Started (not awaited) with ABANDON — the drain runs for hours and must outlive
  // this workflow; the state watermarks make the pass self-advancing regardless.
  await startChild(ingestPackagistMetadata, {
    args: [{}],
    parentClosePolicy: ParentClosePolicy.ABANDON,
  })
}

export async function ingestPackagistMetadata(state: MetadataState = {}): Promise<void> {
  let cursor = state.cursor || ''
  const stopAfterFirstPage = await acts.packagistStopAfterFirstPage()

  for (let r = 0; r < ROUNDS_PER_RUN; r++) {
    const { candidates, nextCursor } = await acts.getPackagistMetadataBatch(cursor, INGEST_BATCH)
    if (candidates.length === 0) return
    await acts.ingestPackagistMetadataBatch(candidates)
    cursor = nextCursor
    if (stopAfterFirstPage) return
    if (candidates.length < INGEST_BATCH) return
  }

  await continueAsNew<typeof ingestPackagistMetadata>({ cursor })
}

// Monthly capture of the observed rolling 30d window for every packagist package.
// The cutoff is fixed once per run (deterministic activity) so the watermark-based
// due-selection drains the whole universe exactly once per cron fire.
export async function ingestPackagistDownloads30d(state: DownloadsState = {}): Promise<void> {
  const cutoff = state.cutoff ?? (await acts.packagistCurrentTimestamp())
  const stopAfterFirstPage = await acts.packagistStopAfterFirstPage()

  for (let r = 0; r < ROUNDS_PER_RUN; r++) {
    const purls = await acts.getPackagist30dBatch(cutoff, INGEST_BATCH)
    if (purls.length === 0) return
    await acts.ingestPackagist30dBatch(purls)
    if (stopAfterFirstPage) return
    if (purls.length < INGEST_BATCH) return
  }

  await continueAsNew<typeof ingestPackagistDownloads30d>({ cutoff })
}

// Daily downloads capture for the critical slice.
export async function ingestPackagistDownloadsDaily(state: DownloadsState = {}): Promise<void> {
  if ((await acts.getCriticalPackagistCount()) === 0) return

  const cutoff = state.cutoff ?? (await acts.packagistCurrentTimestamp())
  // Packagist's `daily` figure is tied to a specific calendar day (see schedule.ts) —
  // derive the write-date from the run's fixed cutoff instead of re-reading the clock
  // per batch, so a drain that runs past UTC midnight still tags every row consistently.
  const runDate = cutoff.slice(0, 10)
  const stopAfterFirstPage = await acts.packagistStopAfterFirstPage()

  for (let r = 0; r < ROUNDS_PER_RUN; r++) {
    const candidates = await acts.getPackagistDailyBatch(cutoff, INGEST_BATCH)
    if (candidates.length === 0) return
    await acts.ingestPackagistDailyBatch(candidates, runDate)
    if (stopAfterFirstPage) return
    if (candidates.length < INGEST_BATCH) return
  }

  await continueAsNew<typeof ingestPackagistDownloadsDaily>({ cutoff })
}
