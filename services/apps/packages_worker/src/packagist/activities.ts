import { Context } from '@temporalio/activity'

import { partition, timeout } from '@crowd/common'
import {
  getCriticalPackagistPackageCount,
  getPackagist30dDuePurls,
  getPackagistDailyDownloadsDue,
  getPackagistMetadataDuePurls,
  insertDailyDownloads,
  insertPackagistPackages,
  logAuditFieldChanges,
  markPackagist30dProcessed,
  markPackagistDailyProcessed,
  markPackagistMetadataScanned,
} from '@crowd/data-access-layer/src/packages'
import type {
  PackagistDailyCandidate,
  PackagistMetadataCandidate,
  PackagistRunResult,
} from '@crowd/data-access-layer/src/packages/packagistPackageState'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

import { getPackagesDb } from '../db'
import { mapWithConcurrency } from '../utils/concurrency'
import { isClientError } from '../utils/isClientError'

import { persistPackagist30dWindow } from './downloads'
import { expandComposerMetadata } from './expandMetadata'
import { fetchPackagistP2, fetchPackagistStats } from './fetchPackage'
import { fetchPackagistPackageList, parsePackagistPackageList } from './listPackages'
import { normalizePackagistStats, packagistNameFromPurl } from './normalize'
import { INGEST_MAX_ATTEMPTS } from './retryPolicy'
import { FetchError, isFetchError, isP2NotModified } from './types'
import { persistPackagistMetadata } from './upsertMetadata'
import { persistPackagistPackageInfo } from './upsertPackageInfo'

const log = getServiceChildLogger('packagist')

const WORKER = 'packagist'

// 4xx/malformed get a few quick in-lane retries with a small linear backoff
const INGEST_4XX_ATTEMPTS = 3
const INGEST_4XX_BACKOFF_MS = 1000

// Concurrency cap for the dynamic (packagist.org) endpoint, shared by all lanes
// that fetch it: metadata, downloads-30d, and daily downloads.
function statsConcurrency(): number {
  const n = parseInt(process.env.CROWD_PACKAGES_PACKAGIST_STATS_CONCURRENCY ?? '10', 10)
  return Math.max(1, Math.min(10, Number.isFinite(n) ? n : 10))
}

function metadataRefreshDays(): number {
  const n = parseInt(process.env.CROWD_PACKAGES_PACKAGIST_METADATA_REFRESH_DAYS ?? '7', 10)
  return Number.isFinite(n) && n > 0 ? n : 7
}

// Scope of the metadata sweep. Deliberately diverges from pypi (critical-only steady
// state): Packagist enriches ALL packages because deps.dev has no Packagist data to
// fall back on. Set CROWD_PACKAGES_PACKAGIST_RUN_ONLY_FOR_CRITICAL=true to narrow
// back to the critical slice.
function runOnlyForCritical(): boolean {
  const raw = (process.env.CROWD_PACKAGES_PACKAGIST_RUN_ONLY_FOR_CRITICAL ?? 'false')
    .trim()
    .toLowerCase()
  return raw === 'true' || raw === '1' || raw === 'yes'
}

export async function packagistStopAfterFirstPage(): Promise<boolean> {
  const raw = (process.env.CROWD_PACKAGES_PACKAGIST_STOP_AFTER_FIRST_PAGE ?? 'false')
    .trim()
    .toLowerCase()
  return raw === 'true' || raw === '1' || raw === 'yes'
}

// Deterministic cutoff source for the watermark-draining download workflows.
export async function packagistCurrentTimestamp(): Promise<string> {
  return new Date().toISOString()
}

// Fetch with the shared fast-retry contract: transient/429 results throw so Temporal
// retries the batch; 4xx/malformed get INGEST_4XX_ATTEMPTS quick in-lane retries and
// then surface as a give-up `error` the caller records on the state row.
async function fetchWithFastRetry<T>(
  fetchOnce: () => Promise<T | FetchError>,
  what: string,
): Promise<{ value: T; attempts: number } | { error: FetchError; attempts: number }> {
  for (let attempt = 1; ; attempt++) {
    const result = await fetchOnce()

    if (!isFetchError(result)) {
      return { value: result, attempts: attempt }
    }

    if (!isClientError(result.statusCode, result.kind) && result.kind !== 'MALFORMED') {
      throw new Error(`Failed to fetch ${what}: ${result.message}`)
    }

    if (attempt >= INGEST_4XX_ATTEMPTS) {
      return { error: result, attempts: attempt }
    }

    await timeout(attempt * INGEST_4XX_BACKOFF_MS)
  }
}

function giveUpResult(error: FetchError): PackagistRunResult {
  return {
    status: 'error',
    attempts: INGEST_4XX_ATTEMPTS,
    httpStatus: error.statusCode,
    errorKind: error.kind,
    message: error.message,
  }
}

// The merged enrichment lane: one pass per package fetches BOTH registry endpoints —
// the dynamic one (package info, repo link, maintainers) and p2 (versions, dependencies).
export async function ingestOnePackagistMetadata(
  qx: QueryExecutor,
  candidate: PackagistMetadataCandidate,
): Promise<void> {
  const name = packagistNameFromPurl(candidate.purl)

  // Phase 1: dynamic endpoint
  const info = await fetchWithFastRetry(
    () => fetchPackagistStats(name),
    `Packagist stats for ${name}`,
  )
  if ('error' in info) {
    log.warn(
      { purl: candidate.purl, statusCode: info.error.statusCode, kind: info.error.kind },
      'packagist package info 4xx/malformed after fast retries — marking scanned and skipping',
    )
    await markPackagistMetadataScanned(qx, candidate.purl, giveUpResult(info.error))
    return
  }

  const stats = normalizePackagistStats(info.value.package)
  const persistedInfo = await persistPackagistPackageInfo(qx, candidate.purl, stats)
  const changedFields = [...persistedInfo.changedFields]

  // Phase 2: p2 endpoint
  const p2 = await fetchWithFastRetry(
    () => fetchPackagistP2(name, candidate.metadataLastModified),
    `Packagist metadata for ${name}`,
  )
  if ('error' in p2) {
    log.warn(
      { purl: candidate.purl, statusCode: p2.error.statusCode, kind: p2.error.kind },
      'packagist metadata 4xx/malformed after fast retries — marking scanned and skipping',
    )
    // Phase-1 writes are already committed — their audit rows must not be dropped.
    await logAuditFieldChanges(qx, WORKER, candidate.purl, changedFields)
    await markPackagistMetadataScanned(qx, candidate.purl, giveUpResult(p2.error))
    return
  }

  let lastModified: string | null = null
  if (!isP2NotModified(p2.value)) {
    const expanded = expandComposerMetadata(p2.value.minifiedVersions)
    const persistResult = await persistPackagistMetadata(qx, candidate.purl, expanded)
    if (persistResult.unresolvedDependencyTargets > 0) {
      log.debug(
        { purl: candidate.purl, unresolved: persistResult.unresolvedDependencyTargets },
        'packagist dependency targets not found in packages — edges skipped',
      )
    }
    changedFields.push(...persistResult.changedFields)
    lastModified = p2.value.lastModified
  }

  await logAuditFieldChanges(qx, WORKER, candidate.purl, changedFields)
  if (lastModified) {
    await markPackagistMetadataScanned(
      qx,
      candidate.purl,
      { status: 'success', attempts: p2.attempts },
      lastModified,
    )
  } else {
    await markPackagistMetadataScanned(qx, candidate.purl, {
      status: 'success',
      attempts: p2.attempts,
    })
  }
}

// The monthly downloads-30d lane: dynamic fetch, one window row per purl per month.
export async function ingestOnePackagist30dWindow(
  qx: QueryExecutor,
  purl: string,
  runDate: string,
): Promise<void> {
  const name = packagistNameFromPurl(purl)

  const info = await fetchWithFastRetry(
    () => fetchPackagistStats(name),
    `Packagist stats for ${name}`,
  )
  if ('error' in info) {
    log.warn(
      { purl, statusCode: info.error.statusCode, kind: info.error.kind },
      'packagist 30d downloads 4xx/malformed after fast retries — marking processed and skipping',
    )
    await markPackagist30dProcessed(qx, purl, giveUpResult(info.error))
    return
  }

  await persistPackagist30dWindow(qx, purl, info.value.package.downloads?.monthly ?? null, runDate)
  await markPackagist30dProcessed(qx, purl, { status: 'success', attempts: info.attempts })
}

// The daily downloads lane (critical slice): dynamic fetch, one downloads_daily row.
export async function ingestOnePackagistDailyDownload(
  qx: QueryExecutor,
  candidate: PackagistDailyCandidate,
  runDate: string,
): Promise<void> {
  const name = packagistNameFromPurl(candidate.purl)

  const info = await fetchWithFastRetry(
    () => fetchPackagistStats(name),
    `Packagist stats for ${name}`,
  )
  if ('error' in info) {
    log.warn(
      { purl: candidate.purl, statusCode: info.error.statusCode, kind: info.error.kind },
      'packagist daily downloads 4xx/malformed after fast retries — marking processed and skipping',
    )
    await markPackagistDailyProcessed(qx, candidate.purl, giveUpResult(info.error))
    return
  }

  const daily = info.value.package.downloads?.daily
  if (typeof daily === 'number') {
    await insertDailyDownloads(qx, candidate.packageId, [{ day: runDate, downloads: daily }])
  }
  await markPackagistDailyProcessed(qx, candidate.purl, {
    status: 'success',
    attempts: info.attempts,
  })
}

export async function ingestPackagistItemsConcurrently<T>(
  items: T[],
  attempt: number,
  concurrency: number,
  ingest: (item: T) => Promise<void>,
  onGiveUp: (item: T, err: unknown) => Promise<void>,
): Promise<void> {
  await mapWithConcurrency(items, concurrency, async (item) => {
    try {
      await ingest(item)
    } catch (err) {
      // Retry via Temporal while attempts remain; then give up and continue
      if (attempt < INGEST_MAX_ATTEMPTS) throw err
      log.warn(
        { item: String(item), attempt, err: String(err) },
        'packagist item failed after max attempts — giving up',
      )
      await onGiveUp(item, err)
    }
  })
}

export async function runPackagistPackageSeed(): Promise<{ discovered: number; invalid: number }> {
  const result = await fetchPackagistPackageList()

  if (isFetchError(result)) {
    throw new Error(`Failed to fetch Packagist package list: ${result.message}`)
  }

  const { entries, invalid } = parsePackagistPackageList(result)

  if (entries.length > 0) {
    const qx = await getPackagesDb()
    for (const chunk of partition(entries, 5000)) {
      await insertPackagistPackages(qx, chunk)
    }
  }

  return { discovered: entries.length, invalid }
}

export async function getPackagistMetadataBatch(
  afterPurl: string,
  batchSize: number,
): Promise<{ candidates: PackagistMetadataCandidate[]; nextCursor: string }> {
  const qx = await getPackagesDb()
  const candidates = await getPackagistMetadataDuePurls(
    qx,
    afterPurl,
    batchSize,
    metadataRefreshDays(),
    runOnlyForCritical(),
  )
  return {
    candidates,
    nextCursor: candidates.length ? candidates[candidates.length - 1].purl : afterPurl,
  }
}

export async function ingestPackagistMetadataBatch(
  candidates: PackagistMetadataCandidate[],
): Promise<void> {
  if (candidates.length === 0) return
  const qx = await getPackagesDb()
  const attempt = Context.current().info.attempt

  // The merged lane starts every ingest with a DYNAMIC-endpoint fetch, so it is
  // bounded by that endpoint's 10-concurrent limit — not p2's 20. Running hotter
  // gets connections reset by packagist.org ("fetch failed").
  await ingestPackagistItemsConcurrently(
    candidates,
    attempt,
    statsConcurrency(),
    (candidate) => ingestOnePackagistMetadata(qx, candidate),
    (candidate, err) =>
      markPackagistMetadataScanned(qx, candidate.purl, {
        status: 'error',
        attempts: attempt,
        message: String(err),
      }),
  )

  log.info({ count: candidates.length }, 'Ingested Packagist metadata batch')
}

export async function getPackagist30dBatch(cutoff: string, batchSize: number): Promise<string[]> {
  const qx = await getPackagesDb()
  return getPackagist30dDuePurls(qx, cutoff, batchSize)
}

export async function ingestPackagist30dBatch(purls: string[]): Promise<void> {
  if (purls.length === 0) return
  const qx = await getPackagesDb()
  const runDate = new Date().toISOString().slice(0, 10)
  const attempt = Context.current().info.attempt

  await ingestPackagistItemsConcurrently(
    purls,
    attempt,
    statsConcurrency(),
    (purl) => ingestOnePackagist30dWindow(qx, purl, runDate),
    (purl, err) =>
      markPackagist30dProcessed(qx, purl, {
        status: 'error',
        attempts: attempt,
        message: String(err),
      }),
  )

  log.info({ count: purls.length }, 'Ingested Packagist 30d downloads batch')
}

export async function getPackagistDailyBatch(
  cutoff: string,
  batchSize: number,
): Promise<PackagistDailyCandidate[]> {
  const qx = await getPackagesDb()
  return getPackagistDailyDownloadsDue(qx, cutoff, batchSize)
}

export async function ingestPackagistDailyBatch(
  candidates: PackagistDailyCandidate[],
): Promise<void> {
  if (candidates.length === 0) return
  const qx = await getPackagesDb()
  const runDate = new Date().toISOString().slice(0, 10)
  const attempt = Context.current().info.attempt

  await ingestPackagistItemsConcurrently(
    candidates,
    attempt,
    statsConcurrency(),
    (candidate) => ingestOnePackagistDailyDownload(qx, candidate, runDate),
    (candidate, err) =>
      markPackagistDailyProcessed(qx, candidate.purl, {
        status: 'error',
        attempts: attempt,
        message: String(err),
      }),
  )

  log.info({ count: candidates.length }, 'Ingested Packagist daily downloads batch')
}

export async function getCriticalPackagistCount(): Promise<number> {
  const qx = await getPackagesDb()
  return getCriticalPackagistPackageCount(qx)
}
