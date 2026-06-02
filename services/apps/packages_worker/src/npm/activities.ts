import {
  getExistingLast30dEndDates,
  getMissingDownloadDates,
  getNpmChangesLastSeq,
  getScannedNpmPackages,
  getTrackedNpmPackages,
  insertDailyDownloads,
  logAuditFieldChanges,
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

export async function getDailyDownloadsTrackedList(): Promise<
  Array<{ id: string; name: string; purl: string; firstReleaseAt: string | null }>
> {
  const watchList = await getWatchList()
  if (watchList.length === 0) return []

  const qx = await getPackagesDb()
  const rows = await getTrackedNpmPackages(qx, watchList.map(buildPurl))

  if (rows.length === 0) {
    log.info('No tracked packages found in packages table yet — skipping daily downloads backfill')
  }

  return rows
}

function utcYesterday(): string {
  const d = new Date()
  d.setUTCDate(d.getUTCDate() - 1)
  return d.toISOString().slice(0, 10)
}

export async function findMissingDownloadWindows(
  packageId: string,
  firstReleaseAt: string | null,
): Promise<Array<{ start: string; end: string }>> {
  const lower =
    firstReleaseAt && firstReleaseAt.slice(0, 10) > NPM_EARLIEST
      ? firstReleaseAt.slice(0, 10)
      : NPM_EARLIEST
  const yesterday = utcYesterday()

  if (lower > yesterday) return []

  const qx = await getPackagesDb()
  const missingDates = await getMissingDownloadDates(qx, packageId, lower, yesterday)
  return computeChunks(missingDates)
}

export async function fetchAndPersistDailyDownloads(
  name: string,
  packageId: string,
  purl: string,
  start: string,
  end: string,
): Promise<number> {
  await sleep(downloadSleepMs())
  const result = await fetchDailyRangeHttp(name, start, end)
  if (isFetchError(result)) {
    throw new Error(
      `Failed to fetch daily downloads for ${name} [${start}:${end}]: ${result.message}`,
    )
  }
  if (result.downloads.length === 0) return 0
  const qx = await getPackagesDb()
  const changedFields = await insertDailyDownloads(qx, packageId, result.downloads)
  await logAuditFieldChanges(qx, WORKER, purl, changedFields)
  return result.downloads.length
}

export async function getDownloadsConcurrency(): Promise<number> {
  const raw = process.env.CROWD_PACKAGES_DOWNLOADS_CONCURRENCY
  const n = parseInt(raw ?? '1', 10)
  return Number.isFinite(n) && n > 0 ? n : 1
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

export async function findMissingLast30dWindows(
  purl: string,
  firstReleaseAt: string | null,
): Promise<Last30dWindow[]> {
  const upperEndDate = utcFirstOfCurrentMonth()
  const qx = await getPackagesDb()
  const existing = await getExistingLast30dEndDates(qx, purl, NPM_EARLIEST, upperEndDate)
  return computeMissingLast30dWindows(firstReleaseAt, upperEndDate, existing)
}

export async function fetchAndPersistLast30dWindow(
  name: string,
  purl: string,
  start: string,
  end: string,
  mirrorToUniverse: boolean,
): Promise<number> {
  await sleep(downloadSleepMs())
  const result = await fetchPointRange(name, start, end)
  if (isFetchError(result)) {
    throw new Error(
      `Failed to fetch last-30d downloads for ${name} [${start}:${end}]: ${result.message}`,
    )
  }
  const qx = await getPackagesDb()
  const changedFields = await upsertLast30dDownload(
    qx,
    purl,
    start,
    end,
    result.count,
    mirrorToUniverse,
  )
  await logAuditFieldChanges(qx, WORKER, purl, changedFields)
  return result.count
}

export async function fetchBulkAndPersistLast30dWindow(
  names: string[],
  purls: string[],
  start: string,
  end: string,
  mirrorToUniverse: boolean,
): Promise<number> {
  await sleep(downloadSleepMs())
  const result = await fetchBulkPointRange(names, start, end)
  if (isFetchError(result)) {
    throw new Error(`Failed to fetch bulk last-30d downloads [${start}:${end}]: ${result.message}`)
  }
  const qx = await getPackagesDb()
  let persisted = 0
  for (let i = 0; i < names.length; i++) {
    const name = names[i]
    const purl = purls[i]
    const count = result.counts.get(name)
    if (count === undefined) {
      log.warn({ name, start, end }, 'npm bulk response: no data for package — skipping')
      continue
    }
    const changedFields = await upsertLast30dDownload(qx, purl, start, end, count, mirrorToUniverse)
    await logAuditFieldChanges(qx, WORKER, purl, changedFields)
    persisted++
  }
  return persisted
}
