import {
  getMissingDownloadDates,
  getNpmChangesLastSeq,
  getPurlsMissingLast30dWindow,
  getScannedNpmPackages,
  getTrackedPackagesNeedingDailyBackfill,
  insertDailyDownloads,
  logAuditFieldChanges,
  markDailyDownloadsProcessed,
  markNpmPackageScanned,
  setNpmChangesLastSeq,
  upsertLast30dDownload,
} from '@crowd/data-access-layer/src/packages'
import { getServiceChildLogger } from '@crowd/logging'

import { getPackagesDb } from '../db'

import { NPM_EARLIEST, computeChunks } from './downloadGaps'
import { fetchChangesSince, fetchCurrentSeq } from './fetchChanges'
import {
  fetchBulkPointRange,
  fetchDailyRange as fetchDailyRangeHttp,
  fetchPointRange,
} from './fetchDownloads'
import { fetchPackument } from './fetchPackument'
import { Last30dWindow, computeMissingLast30dWindows } from './last30dGaps'
import { buildPurl } from './normalize'
import { isFetchError } from './types'
import { upsertPackage } from './upsertPackage'
import { getWatchList } from './watchList'

export { getWatchList }

const log = getServiceChildLogger('npm')

const WORKER = 'npm'

export interface PollNpmChangesResult {
  names: string[]
  lastSeq: string
  hasMore: boolean
}

export async function pollNpmChanges(): Promise<PollNpmChangesResult> {
  const qx = await getPackagesDb()

  const since = await getNpmChangesLastSeq(qx)

  if (!since) {
    log.info('npm_worker_state empty — bootstrapping last_seq from replicate.npmjs.com')
    const seqResult = await fetchCurrentSeq()
    if (isFetchError(seqResult)) {
      throw new Error(`Failed to bootstrap npm seq: ${seqResult.message}`)
    }
    log.info({ seq: seqResult }, 'Bootstrapping changes_last_seq')
    return { names: [], lastSeq: seqResult, hasMore: false }
  }

  log.info({ since }, 'Polling npm _changes feed')

  const result = await fetchChangesSince(since)
  if (isFetchError(result)) {
    throw new Error(`Failed to fetch npm changes: ${result.message}`)
  }

  log.info(
    { count: result.names.length, newSeq: result.lastSeq, hasMore: result.hasMore },
    'Fetched npm changes page',
  )
  return { names: result.names, lastSeq: result.lastSeq, hasMore: result.hasMore }
}

// Advance the _changes cursor. Called by the workflow AFTER the page's packages
// are ingested so the cursor never moves past unprocessed work.
export async function commitNpmChangesSeq(lastSeq: string): Promise<void> {
  const qx = await getPackagesDb()
  await setNpmChangesLastSeq(qx, lastSeq)
}

export async function ingestNpmPackage(name: string): Promise<void> {
  const qx = await getPackagesDb()

  const packumentResult = await fetchPackument(name)
  if (isFetchError(packumentResult)) {
    if (packumentResult.kind === 'NOT_FOUND') {
      log.warn({ name }, 'npm package not found — skipping')
      return
    }
    throw new Error(`Failed to fetch packument for ${name}: ${packumentResult.message}`)
  }

  const { purl, changedFields } = await upsertPackage(qx, packumentResult)
  await logAuditFieldChanges(qx, WORKER, purl, changedFields)
  await markNpmPackageScanned(qx, name)
  log.info({ name, changedFields: changedFields.length }, 'Ingested npm package')
}

export async function getUnscannedPackages(names: string[]): Promise<string[]> {
  if (names.length === 0) return []
  const qx = await getPackagesDb()
  const scanned = new Set(await getScannedNpmPackages(qx, names))
  return names.filter((n) => !scanned.has(n))
}

function utcYesterday(): string {
  const d = new Date()
  d.setUTCDate(d.getUTCDate() - 1)
  return d.toISOString().slice(0, 10)
}

export async function currentTimestamp(): Promise<string> {
  return new Date().toISOString()
}

export async function backfillDailyBatch(
  cutoff: string,
  batchSize: number,
): Promise<{ fetched: number }> {
  const watchList = await getWatchList()
  if (watchList.length === 0) return { fetched: 0 }

  const qx = await getPackagesDb()
  const candidates = await getTrackedPackagesNeedingDailyBackfill(
    qx,
    watchList,
    watchList.map(buildPurl),
    cutoff,
    batchSize,
  )
  if (candidates.length === 0) return { fetched: 0 }

  const yesterday = utcYesterday()

  for (const pkg of candidates) {
    const lower =
      pkg.firstReleaseAt && pkg.firstReleaseAt.slice(0, 10) > NPM_EARLIEST
        ? pkg.firstReleaseAt.slice(0, 10)
        : NPM_EARLIEST

    if (lower <= yesterday) {
      const missingDates = await getMissingDownloadDates(qx, pkg.id, lower, yesterday)
      for (const w of computeChunks(missingDates)) {
        await sleep(downloadSleepMs())
        const result = await fetchDailyRangeHttp(pkg.name, w.start, w.end)
        if (isFetchError(result)) {
          throw new Error(
            `Failed to fetch daily downloads for ${pkg.name} [${w.start}:${w.end}]: ${result.message}`,
          )
        }
        if (result.downloads.length === 0) continue
        const changedFields = await insertDailyDownloads(qx, pkg.id, result.downloads)
        await logAuditFieldChanges(qx, WORKER, pkg.purl, changedFields)
      }
    }

    await markDailyDownloadsProcessed(qx, pkg.name)
  }

  return { fetched: candidates.length }
}

function downloadSleepMs(): number {
  const raw = process.env.CROWD_PACKAGES_DOWNLOAD_SLEEP_MS
  const n = parseInt(raw ?? '1000', 10)
  return Number.isFinite(n) && n >= 0 ? n : 500
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function utcFirstOfCurrentMonth(): string {
  const d = new Date()
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1)).toISOString().slice(0, 10)
}

export async function getLast30dWindows(): Promise<Last30dWindow[]> {
  return computeMissingLast30dWindows(null, utcFirstOfCurrentMonth(), [])
}

const PURL_NPM_PREFIX = 'pkg:npm/'

export interface Last30dBatchResult {
  fetched: number
  nextCursor: string
}

export async function refreshLast30dWindowBatch(
  start: string,
  end: string,
  mirrorToUniverse: boolean,
  afterPurl: string,
  batchSize: number,
): Promise<Last30dBatchResult> {
  const watchList = await getWatchList()
  if (watchList.length === 0) return { fetched: 0, nextCursor: afterPurl }

  const qx = await getPackagesDb()
  const missingPurls = await getPurlsMissingLast30dWindow(
    qx,
    watchList.map(buildPurl),
    end,
    afterPurl,
    batchSize,
  )
  if (missingPurls.length === 0) return { fetched: 0, nextCursor: afterPurl }

  // Rows come back ORDER BY purl, so the last is the max → the next cursor.
  const nextCursor = missingPurls[missingPurls.length - 1]

  const items = missingPurls.map((purl) => ({ purl, name: purl.slice(PURL_NPM_PREFIX.length) }))
  const unscoped = items.filter((i) => !i.name.startsWith('@'))
  const scoped = items.filter((i) => i.name.startsWith('@'))

  for (let i = 0; i < unscoped.length; i += 128) {
    const batch = unscoped.slice(i, i + 128)
    await sleep(downloadSleepMs())
    const result = await fetchBulkPointRange(
      batch.map((b) => b.name),
      start,
      end,
    )
    if (isFetchError(result)) {
      throw new Error(
        `Failed to fetch bulk last-30d downloads [${start}:${end}]: ${result.message}`,
      )
    }
    for (const b of batch) {
      const count = result.counts.get(b.name)
      if (count === undefined) {
        log.warn({ name: b.name, start, end }, 'npm bulk response: no data for package — skipping')
        continue
      }
      const changedFields = await upsertLast30dDownload(
        qx,
        b.purl,
        start,
        end,
        count,
        mirrorToUniverse,
      )
      await logAuditFieldChanges(qx, WORKER, b.purl, changedFields)
    }
  }

  for (const s of scoped) {
    await sleep(downloadSleepMs())
    const result = await fetchPointRange(s.name, start, end)
    if (isFetchError(result)) {
      throw new Error(
        `Failed to fetch last-30d downloads for ${s.name} [${start}:${end}]: ${result.message}`,
      )
    }
    const changedFields = await upsertLast30dDownload(
      qx,
      s.purl,
      start,
      end,
      result.count,
      mirrorToUniverse,
    )
    await logAuditFieldChanges(qx, WORKER, s.purl, changedFields)
  }

  return { fetched: missingPurls.length, nextCursor }
}
