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

// The last-30d lane heartbeats after every window, so it gets its own proxy with a
// heartbeatTimeout: a stalled or worker-lost lane is detected in minutes instead of
// waiting out startToClose, and because each lane is bounded (windows capped per round)
// and idempotent (progress lives in downloads_last_30d), the retry resumes cheaply.
const heartbeatingActs = proxyActivities<typeof activities>({
  startToCloseTimeout: '10 minutes',
  heartbeatTimeout: '2 minutes',
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
  //    A page can carry up to PAGE_LIMIT (1000) tracked purls, so drain it in the same
  //    per-round chunks as the backfill axis — otherwise one lane could get the whole
  //    page and blow past the activity timeout (poisoning the page on every retry).
  const changes = await acts.pollNpmChanges()
  if (changes.names.length > 0) {
    const toIngest = await acts.getChangedNpmPurls(changes.names)
    for (let i = 0; i < toIngest.length; i += perRound) {
      await runLanes(shardRoundRobin(toIngest.slice(i, i + perRound), lanes))
    }
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

// ── Breadth: latest-window refresh (monthly) ──────────────────────────────────────────────
// One window per package, so a large batch fills the bulk-128 call. Each round selects a fresh
// shard (the watermark advances per package), draining the universe breadth-first.
const LATEST_PER_LANE = 128
const LATEST_MAX_BATCHES_PER_RUN = 50

interface RefreshLatestLast30dState {
  cutoff?: string
}

// Refresh the current 30-day window for every npm package and mirror it to
// packages.downloads_last_30d. Runs monthly; continues-as-new until every package's
// latest window for this cutoff is done. Deep history is a separate workflow (below).
export async function refreshLatestLast30dDownloads(
  state: RefreshLatestLast30dState = {},
): Promise<void> {
  const cutoff = state.cutoff ?? (await acts.currentTimestamp())
  const lanes = await acts.getLaneCount()

  for (let i = 0; i < LATEST_MAX_BATCHES_PER_RUN; i++) {
    const results = await Promise.all(
      Array.from({ length: lanes }, (_, lane) =>
        heartbeatingActs.refreshLatestLast30dLane(cutoff, LATEST_PER_LANE, lane, lanes),
      ),
    )
    const total = results.reduce((sum, r) => sum + r.fetched, 0)
    if (total === 0) return
  }

  await continueAsNew<typeof refreshLatestLast30dDownloads>({ cutoff })
}

// ── Depth: older-history backfill (daily, background) ──────────────────────────────────────
// The same packages are re-selected across rounds until their backlog drains (the depth
// watermark advances only when a package is fully backfilled), so a round's cost is bounded by
// HISTORY_PER_LANE × HISTORY_WINDOWS_PER_ROUND fetches — short enough for the activity timeout.
const HISTORY_PER_LANE = 50
const HISTORY_WINDOWS_PER_ROUND = 6
const HISTORY_MAX_BATCHES_PER_RUN = 50

// Backfill the older 30-day windows (everything except the latest) for packages whose latest
// window is already present. Runs daily and continues-as-new until the whole universe's history
// is filled; independent of the monthly breadth refresh, so it never blocks it.
export async function backfillLast30dHistory(): Promise<void> {
  const lanes = await acts.getLaneCount()

  for (let i = 0; i < HISTORY_MAX_BATCHES_PER_RUN; i++) {
    const results = await Promise.all(
      Array.from({ length: lanes }, (_, lane) =>
        heartbeatingActs.backfillLast30dHistoryLane(
          HISTORY_PER_LANE,
          HISTORY_WINDOWS_PER_ROUND,
          lane,
          lanes,
        ),
      ),
    )
    const total = results.reduce((sum, r) => sum + r.fetched, 0)
    if (total === 0) return
  }

  await continueAsNew<typeof backfillLast30dHistory>()
}
