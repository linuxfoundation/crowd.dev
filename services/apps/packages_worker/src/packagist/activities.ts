import { ApplicationFailure, Context } from '@temporalio/activity'

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
import {
  createPackagistTransitiveRun,
  failPackagistTransitiveRun as failRunInLedger,
  findUnfinishedPackagistTransitiveRun,
  finishPackagistTransitiveRun as finishRunInLedger,
  markPackagistTransitiveRunMerging,
} from '@crowd/data-access-layer/src/packages/packagistTransitiveRuns'
import {
  EmptyPackagistTransitiveCountsError,
  computePackagistTransitiveCounts,
  mergePackagistTransitiveCounts,
  snapshotPackagistDirectEdges,
} from '@crowd/data-access-layer/src/packages/transitiveDependents'
import type { PackagistTransitiveMergeResult } from '@crowd/data-access-layer/src/packages/transitiveDependents'
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
import { INGEST_MAX_ATTEMPTS, TRANSITIVE_PREPARE_MAX_ATTEMPTS } from './retryPolicy'
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

// Current Temporal attempt, defaulting to 1 when run standalone (tests, scripts).
function activityAttempt(): number {
  try {
    return Context.current().info.attempt
  } catch {
    return 1
  }
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

function giveUpResult(error: FetchError, attempts: number): PackagistRunResult {
  return {
    status: 'error',
    attempts,
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
  // This batch activity's own scheduledTimestampMs (stable across Temporal retries),
  // passed through to every give-up write below — see MarkMetadataScannedOptions.notBefore.
  scheduledAt: string,
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
    await markPackagistMetadataScanned(
      qx,
      candidate.purl,
      giveUpResult(info.error, info.attempts),
      {
        notBefore: scheduledAt,
      },
    )
    return
  }

  const stats = normalizePackagistStats(info.value.package)
  // persistPackagistPackageInfo audits its own writes atomically, inside the same
  // transaction — phase 1 is committed-and-audited before the p2 fetch (which can
  // throw) ever runs.
  await persistPackagistPackageInfo(qx, candidate.purl, stats)

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
    // Phase 1 already succeeded — only p2 (versions/deps) failed to refresh. Don't push
    // metadata_last_run_at forward, or due-selection wrongly treats this package as
    // "recently scanned" and skips it for the full refresh window despite stale p2 data.
    await markPackagistMetadataScanned(qx, candidate.purl, giveUpResult(p2.error, p2.attempts), {
      bumpLastRunAt: false,
      notBefore: scheduledAt,
    })
    return
  }

  let lastModified: string | null = null
  if (!isP2NotModified(p2.value)) {
    const expanded = expandComposerMetadata(p2.value.minifiedVersions)
    // persistPackagistMetadata audits its own writes atomically, inside the same
    // transaction as the aggregate/version/dependency writes.
    const persistResult = await persistPackagistMetadata(qx, candidate.purl, expanded)
    if (persistResult.unresolvedDependencyTargets > 0) {
      log.debug(
        { purl: candidate.purl, unresolved: persistResult.unresolvedDependencyTargets },
        'packagist dependency targets not found in packages — edges skipped',
      )
    }
    lastModified = p2.value.lastModified
  }

  await markPackagistMetadataScanned(
    qx,
    candidate.purl,
    { status: 'success', attempts: p2.attempts },
    {
      metadataLastModified: lastModified,
    },
  )
}

// The monthly downloads-30d lane: dynamic fetch, one window row per purl per month.
export async function ingestOnePackagist30dWindow(
  qx: QueryExecutor,
  purl: string,
  runDate: string,
  scheduledAt: string,
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
    await markPackagist30dProcessed(qx, purl, giveUpResult(info.error, info.attempts), scheduledAt)
    return
  }

  // persistPackagist30dWindow audits its own write atomically, inside the same
  // transaction as the insert-if-absent.
  await persistPackagist30dWindow(qx, purl, info.value.package.downloads?.monthly ?? null, runDate)
  await markPackagist30dProcessed(qx, purl, { status: 'success', attempts: info.attempts })
}

// The daily downloads lane (critical slice): dynamic fetch, one downloads_daily row.
export async function ingestOnePackagistDailyDownload(
  qx: QueryExecutor,
  candidate: PackagistDailyCandidate,
  runDate: string,
  scheduledAt: string,
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
    await markPackagistDailyProcessed(
      qx,
      candidate.purl,
      giveUpResult(info.error, info.attempts),
      scheduledAt,
    )
    return
  }

  const daily = info.value.package.downloads?.daily
  if (typeof daily === 'number') {
    // Insert + audit share one transaction so a failed audit insert can never leave a
    // committed row unaudited — a retry would hit ON CONFLICT DO NOTHING and report no
    // changes, permanently losing the audit event otherwise.
    await qx.tx(async (t) => {
      const changedFields = await insertDailyDownloads(t, candidate.packageId, [
        { day: runDate, downloads: daily },
      ])
      await logAuditFieldChanges(t, WORKER, candidate.purl, changedFields)
    })
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
  // mapWithConcurrency stops scheduling new items after the first rejection from the
  // wrapped callback. `attempt` tracks the whole batch activity, not each item, so a
  // rethrow here on an early item would starve every later item in the array of a
  // genuine try this round. Never rethrow from inside the callback — collect the first
  // retryable failure and decide once, after every item has actually been attempted.
  let firstRetryableError: unknown

  await mapWithConcurrency(items, concurrency, async (item) => {
    try {
      await ingest(item)
    } catch (err) {
      // Retry via Temporal while attempts remain; then give up and continue
      if (attempt < INGEST_MAX_ATTEMPTS) {
        if (firstRetryableError === undefined) firstRetryableError = err
        return
      }
      log.warn(
        { item: String(item), attempt, err: String(err) },
        'packagist item failed after max attempts — giving up',
      )
      await onGiveUp(item, err)
    }
  })

  if (firstRetryableError !== undefined) throw firstRetryableError
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

// `cutoff` is the drain's own fixed start time (stable across every round via
// continueAsNew), not a live NOW() — a keyset scan only visits each purl once per
// drain, so re-deriving "7 days ago" fresh on every batch would silently skip a purl
// that hasn't quite hit the refresh window yet when the cursor passes it, pushing its
// effective cadence out toward two refresh cycles instead of one.
export async function getPackagistMetadataBatch(
  cutoff: string,
  afterPurl: string,
  batchSize: number,
): Promise<{ candidates: PackagistMetadataCandidate[]; nextCursor: string }> {
  const qx = await getPackagesDb()
  const dueCutoff = new Date(
    new Date(cutoff).getTime() - metadataRefreshDays() * 24 * 60 * 60 * 1000,
  ).toISOString()
  const candidates = await getPackagistMetadataDuePurls(
    qx,
    dueCutoff,
    afterPurl,
    batchSize,
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
  // Stable across every Temporal retry of this same batch — see notBefore below.
  const scheduledAt = new Date(Context.current().info.scheduledTimestampMs).toISOString()

  // The merged lane starts every ingest with a DYNAMIC-endpoint fetch, so it is
  // bounded by that endpoint's 10-concurrent limit — not p2's 20. Running hotter
  // gets connections reset by packagist.org ("fetch failed").
  await ingestPackagistItemsConcurrently(
    candidates,
    attempt,
    statsConcurrency(),
    (candidate) => ingestOnePackagistMetadata(qx, candidate, scheduledAt),
    (candidate, err) =>
      markPackagistMetadataScanned(
        qx,
        candidate.purl,
        { status: 'error', attempts: attempt, message: String(err) },
        {
          // An item that already succeeded earlier in this same batch's retry
          // sequence must not have that success overwritten by an unrelated
          // re-processing failure.
          notBefore: scheduledAt,
          // This generic catch-all fires whenever ingestOnePackagistMetadata threw
          // (its own classified give-up paths return normally instead) — meaning we
          // can't tell whether phase 1 alone succeeded before a transient p2 failure
          // exhausted Temporal's retries. Never bump the refresh watermark here, or a
          // genuine p2/versions failure gets hidden for the full refresh window.
          bumpLastRunAt: false,
        },
      ),
  )

  log.info({ count: candidates.length }, 'Ingested Packagist metadata batch')
}

export async function getPackagist30dBatch(
  cutoff: string,
  afterPurl: string,
  batchSize: number,
): Promise<{ purls: string[]; nextCursor: string }> {
  const qx = await getPackagesDb()
  const purls = await getPackagist30dDuePurls(qx, cutoff, afterPurl, batchSize)
  return { purls, nextCursor: purls.length ? purls[purls.length - 1] : afterPurl }
}

export async function ingestPackagist30dBatch(purls: string[], runDate: string): Promise<void> {
  if (purls.length === 0) return
  const qx = await getPackagesDb()
  const attempt = Context.current().info.attempt
  const scheduledAt = new Date(Context.current().info.scheduledTimestampMs).toISOString()

  await ingestPackagistItemsConcurrently(
    purls,
    attempt,
    statsConcurrency(),
    (purl) => ingestOnePackagist30dWindow(qx, purl, runDate, scheduledAt),
    (purl, err) =>
      markPackagist30dProcessed(
        qx,
        purl,
        { status: 'error', attempts: attempt, message: String(err) },
        scheduledAt,
      ),
  )

  log.info({ count: purls.length }, 'Ingested Packagist 30d downloads batch')
}

export async function getPackagistDailyBatch(
  cutoff: string,
  afterPurl: string,
  batchSize: number,
): Promise<{ candidates: PackagistDailyCandidate[]; nextCursor: string }> {
  const qx = await getPackagesDb()
  const candidates = await getPackagistDailyDownloadsDue(qx, cutoff, afterPurl, batchSize)
  return {
    candidates,
    nextCursor: candidates.length ? candidates[candidates.length - 1].purl : afterPurl,
  }
}

export async function ingestPackagistDailyBatch(
  candidates: PackagistDailyCandidate[],
  runDate: string,
): Promise<void> {
  if (candidates.length === 0) return
  const qx = await getPackagesDb()
  const attempt = Context.current().info.attempt
  const scheduledAt = new Date(Context.current().info.scheduledTimestampMs).toISOString()

  await ingestPackagistItemsConcurrently(
    candidates,
    attempt,
    statsConcurrency(),
    (candidate) => ingestOnePackagistDailyDownload(qx, candidate, runDate, scheduledAt),
    (candidate, err) =>
      markPackagistDailyProcessed(
        qx,
        candidate.purl,
        { status: 'error', attempts: attempt, message: String(err) },
        scheduledAt,
      ),
  )

  log.info({ count: candidates.length }, 'Ingested Packagist daily downloads batch')
}

export async function getCriticalPackagistCount(): Promise<number> {
  const qx = await getPackagesDb()
  return getCriticalPackagistPackageCount(qx)
}

// The heavy phase of the transitive lane: snapshot the direct edges, run the closure,
// leave the run in 'merging' for the keyset drain that follows. The graph sizes land
// on the run row; the workflow only needs the id.
export async function preparePackagistTransitiveCounts(): Promise<{ runId: number }> {
  const qx = await getPackagesDb()

  // On retry, an unfinished row from the prior attempt may already exist — even one
  // already marked 'merging', if the activity completion was lost after the commit.
  // Adopt it; re-marking 'merging' below is idempotent.
  const runId =
    (await findUnfinishedPackagistTransitiveRun(qx)) ?? (await createPackagistTransitiveRun(qx))

  // Both steps are single long DB statements (the snapshot scans all of
  // package_dependencies), so liveness comes from a timer heartbeat rather than
  // per-item progress; the guard keeps the activity runnable standalone.
  const beat = setInterval(() => {
    try {
      Context.current().heartbeat()
    } catch {
      /* standalone */
    }
  }, 30_000)

  try {
    const edgeCount = await snapshotPackagistDirectEdges(qx)
    if (edgeCount === 0) {
      // A genuinely empty graph means upstream ingestion is broken — retrying won't help.
      throw ApplicationFailure.nonRetryable(
        'no packagist direct edges found — snapshot produced an empty graph',
      )
    }
    const packagesWithDependents = await computePackagistTransitiveCounts(qx)
    await markPackagistTransitiveRunMerging(qx, runId, { edgeCount, packagesWithDependents })
    log.info({ runId, edgeCount, packagesWithDependents }, 'packagist transitive closure prepared')
    return { runId }
  } catch (err) {
    // Fail-mark only terminal outcomes: on a retryable error Temporal re-runs this
    // activity, which adopts the same unfinished row — marking it 'failed' early would
    // make it unadoptable and each retry would mint a duplicate.
    const nonRetryable = err instanceof ApplicationFailure && err.nonRetryable
    if (nonRetryable || activityAttempt() >= TRANSITIVE_PREPARE_MAX_ATTEMPTS) {
      await failRunInLedger(qx, runId, (err as Error).message)
    }
    throw err
  } finally {
    clearInterval(beat)
  }
}

export async function mergePackagistTransitiveBatch(
  afterId: string,
  limit: number,
): Promise<PackagistTransitiveMergeResult> {
  const qx = await getPackagesDb()
  try {
    return await mergePackagistTransitiveCounts(qx, afterId, limit)
  } catch (err) {
    // An empty counts table cannot heal by retrying — fail fast so the workflow
    // fail-marks the run instead of burning the retry schedule against it.
    if (err instanceof EmptyPackagistTransitiveCountsError) {
      throw ApplicationFailure.nonRetryable(err.message)
    }
    throw err
  }
}

export async function finishPackagistTransitiveRun(
  runId: number,
  totals: { processed: number; changed: number },
): Promise<void> {
  const qx = await getPackagesDb()
  await finishRunInLedger(qx, runId, totals)
}

// Terminal failure marking for the merge phase — called from the workflow's catch so a
// permanently failed drain reads 'failed' instead of sitting in 'merging' forever.
export async function failPackagistTransitiveRun(
  runId: number,
  errorMessage: string,
): Promise<void> {
  const qx = await getPackagesDb()
  await failRunInLedger(qx, runId, errorMessage)
}
