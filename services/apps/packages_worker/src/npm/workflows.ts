import { proxyActivities } from '@temporalio/workflow'

import { mapWithConcurrency } from '../utils/concurrency'

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
  const [watchList, changedNames] = await Promise.all([acts.getWatchList(), acts.pollNpmChanges()])

  const newPackages = await acts.getUnscannedPackages(watchList)
  for (const name of newPackages) {
    await acts.ingestNpmPackage(name)
  }

  const scanned = new Set(newPackages)
  const watchSet = new Set(watchList)
  const toIngest = changedNames.filter((n) => watchSet.has(n) && !scanned.has(n))
  for (const name of toIngest) {
    await acts.ingestNpmPackage(name)
  }
}

export async function backfillDailyDownloads(): Promise<void> {
  const [tracked, concurrency] = await Promise.all([
    acts.getDailyDownloadsTrackedList(),
    acts.getDownloadsConcurrency(),
  ])

  await mapWithConcurrency(tracked, concurrency, async (pkg) => {
    const windows = await acts.findMissingDownloadWindows(pkg.id, pkg.firstReleaseAt)
    for (const w of windows) {
      await acts.fetchAndPersistDailyDownloads(pkg.name, pkg.id, pkg.purl, w.start, w.end)
    }
  })
}

export async function refreshLast30dDownloads(): Promise<void> {
  const [tracked, concurrency] = await Promise.all([
    acts.getDailyDownloadsTrackedList(),
    acts.getDownloadsConcurrency(),
  ])

  // Step 1: per-package gap detection in parallel
  const gaps = await mapWithConcurrency(tracked, concurrency, async (pkg) => ({
    name: pkg.name,
    purl: pkg.purl,
    windows: await acts.findMissingLast30dWindows(pkg.purl, pkg.firstReleaseAt),
  }))

  // Step 2: group by window key — pure Map/Array ops, no I/O, determinism preserved
  type WindowEntry = {
    start: string
    end: string
    isLatest: boolean
    unscoped: Array<{ name: string; purl: string }>
    scoped: Array<{ name: string; purl: string }>
  }
  const byWindow = new Map<string, WindowEntry>()
  for (const { name, purl, windows } of gaps) {
    for (const w of windows) {
      const key = `${w.start}:${w.end}`
      let entry = byWindow.get(key)
      if (!entry) {
        entry = { start: w.start, end: w.end, isLatest: w.isLatest, unscoped: [], scoped: [] }
        byWindow.set(key, entry)
      }
      ;(name.startsWith('@') ? entry.scoped : entry.unscoped).push({ name, purl })
    }
  }

  // Step 3: process windows with concurrency=2 — bulk for unscoped (max 128 per call), individual for scoped
  await mapWithConcurrency(Array.from(byWindow.values()), 2, async (entry) => {
    for (let i = 0; i < entry.unscoped.length; i += 128) {
      const batch = entry.unscoped.slice(i, i + 128)
      await acts.fetchBulkAndPersistLast30dWindow(
        batch.map((p) => p.name),
        batch.map((p) => p.purl),
        entry.start,
        entry.end,
        entry.isLatest,
      )
    }
    for (const { name, purl } of entry.scoped) {
      await acts.fetchAndPersistLast30dWindow(name, purl, entry.start, entry.end, entry.isLatest)
    }
  })
}
