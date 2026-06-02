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

export async function ingestNpmPackages(): Promise<void> {
  const [watchList, changes] = await Promise.all([acts.getWatchList(), acts.pollNpmChanges()])

  const newPackages = await acts.getUnscannedPackages(watchList)
  for (const name of newPackages) {
    await acts.ingestNpmPackage(name)
  }

  const scanned = new Set(newPackages)
  const watchSet = new Set(watchList)
  const toIngest = changes.names.filter((n) => watchSet.has(n) && !scanned.has(n))
  for (const name of toIngest) {
    await acts.ingestNpmPackage(name)
  }

  // Advance the cursor only after this page is ingested
  await acts.commitNpmChangesSeq(changes.lastSeq)

  if (changes.hasMore) {
    await continueAsNew<typeof ingestNpmPackages>()
  }
}

const DAILY_BATCH_SIZE = 20
const DAILY_MAX_BATCHES_PER_RUN = 50

interface BackfillDailyState {
  cutoff?: string
}

export async function backfillDailyDownloads(state: BackfillDailyState = {}): Promise<void> {
  const cutoff = state.cutoff ?? (await acts.currentTimestamp())

  for (let i = 0; i < DAILY_MAX_BATCHES_PER_RUN; i++) {
    const { fetched } = await acts.backfillDailyBatch(cutoff, DAILY_BATCH_SIZE)
    if (fetched === 0) return
  }

  await continueAsNew<typeof backfillDailyDownloads>({ cutoff })
}

const LAST30D_BATCH_SIZE = 1000
const LAST30D_MAX_BATCHES_PER_RUN = 50

interface RefreshLast30dState {
  windowIndex: number
  cursor: string
}

export async function refreshLast30dDownloads(
  state: RefreshLast30dState = { windowIndex: 0, cursor: '' },
): Promise<void> {
  const windows = await acts.getLast30dWindows()

  let { windowIndex, cursor } = state
  let batches = 0

  while (windowIndex < windows.length) {
    const w = windows[windowIndex]
    const { fetched, nextCursor } = await acts.refreshLast30dWindowBatch(
      w.start,
      w.end,
      w.isLatest,
      cursor,
      LAST30D_BATCH_SIZE,
    )
    batches++

    if (fetched < LAST30D_BATCH_SIZE) {
      // Window drained for this run → advance to the next window.
      windowIndex++
      cursor = ''
    } else {
      cursor = nextCursor
    }

    if (batches >= LAST30D_MAX_BATCHES_PER_RUN && windowIndex < windows.length) {
      await continueAsNew<typeof refreshLast30dDownloads>({ windowIndex, cursor })
    }
  }
}
