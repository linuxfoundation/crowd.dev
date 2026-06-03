import { continueAsNew, proxyActivities } from '@temporalio/workflow'

import type * as activities from './activities'

const acts = proxyActivities<typeof activities>({
  startToCloseTimeout: '10 minutes',
  retry: {
    initialInterval: '30 seconds',
    backoffCoefficient: 2,
    maximumAttempts: 5,
  },
})

// Per lane, per round. Total purls fetched per round = lanes × INGEST_PER_LANE.
const INGEST_PER_LANE = 50
const INGEST_ROUNDS_PER_RUN = 25

interface IngestState {
  unscannedCursor: string
  unscannedDone: boolean
}

// Deterministic round-robin split of items into `lanes` shards.
function shardRoundRobin<T>(items: T[], lanes: number): T[][] {
  const shards: T[][] = Array.from({ length: lanes }, () => [])
  for (let i = 0; i < items.length; i++) shards[i % lanes].push(items[i])
  return shards
}

// Run each non-empty shard as its own ingest lane (one proxy IP each), concurrently.
async function runLanes(shards: string[][]): Promise<void> {
  await Promise.all(
    shards.map((shard, i) => (shard.length ? acts.ingestNpmPackageBatch(shard, i) : undefined)),
  )
}

export async function ingestNpmPackages(
  state: IngestState = { unscannedCursor: '', unscannedDone: false },
): Promise<void> {
  let { unscannedCursor, unscannedDone } = state

  // Concurrency = number of lanes (one per proxy IP when enabled, else 1).
  const lanes = await acts.getLaneCount()
  const perRound = lanes * INGEST_PER_LANE

  // 1. Drain unscanned npm packages, fanning each round out across the lanes.
  if (!unscannedDone) {
    for (let r = 0; r < INGEST_ROUNDS_PER_RUN; r++) {
      const { purls, nextCursor } = await acts.getUnscannedNpmBatch(unscannedCursor, perRound)
      await runLanes(shardRoundRobin(purls, lanes))
      unscannedCursor = nextCursor
      if (purls.length < perRound) {
        unscannedDone = true
        break
      }
    }
  }

  // 2. Process exactly one _changes page, re-ingesting tracked packages across lanes.
  const changes = await acts.pollNpmChanges()
  if (changes.names.length > 0) {
    const toIngest = await acts.getChangedNpmPurls(changes.names)
    await runLanes(shardRoundRobin(toIngest, lanes))
  }

  // Advance the cursor only after this page is ingested.
  await acts.commitNpmChangesSeq(changes.lastSeq)

  // 3. Continue while either axis still has work; the changes cursor advances every
  //    run so a long backfill never starves the feed.
  if (!unscannedDone || changes.hasMore) {
    await continueAsNew<typeof ingestNpmPackages>({ unscannedCursor, unscannedDone })
  }
}

// Per lane, per round. Total packages selected per round = lanes × *_PER_LANE.
const DAILY_PER_LANE = 20
const DAILY_MAX_BATCHES_PER_RUN = 50

interface BackfillDailyState {
  cutoff?: string
}

export async function backfillDailyDownloads(state: BackfillDailyState = {}): Promise<void> {
  const cutoff = state.cutoff ?? (await acts.currentTimestamp())
  const lanes = await acts.getLaneCount()

  // Each round fans out one self-selecting lane per proxy IP; lanes drain disjoint
  // hash-shards of the due set. A round with no work across all lanes means done.
  for (let i = 0; i < DAILY_MAX_BATCHES_PER_RUN; i++) {
    const results = await Promise.all(
      Array.from({ length: lanes }, (_, lane) =>
        acts.backfillDailyLane(cutoff, DAILY_PER_LANE, lane, lanes),
      ),
    )
    const total = results.reduce((sum, r) => sum + r.fetched, 0)
    if (total === 0) return
  }

  await continueAsNew<typeof backfillDailyDownloads>({ cutoff })
}

const LAST30D_PER_LANE = 1000
const LAST30D_MAX_BATCHES_PER_RUN = 50

interface RefreshLast30dState {
  cutoff?: string
}

export async function refreshLast30dDownloads(state: RefreshLast30dState = {}): Promise<void> {
  const cutoff = state.cutoff ?? (await acts.currentTimestamp())
  const lanes = await acts.getLaneCount()

  for (let i = 0; i < LAST30D_MAX_BATCHES_PER_RUN; i++) {
    const results = await Promise.all(
      Array.from({ length: lanes }, (_, lane) =>
        acts.backfillLast30dLane(cutoff, LAST30D_PER_LANE, lane, lanes),
      ),
    )
    const total = results.reduce((sum, r) => sum + r.fetched, 0)
    if (total === 0) return
  }

  await continueAsNew<typeof refreshLast30dDownloads>({ cutoff })
}
