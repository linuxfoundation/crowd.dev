import { type Dispatcher, ProxyAgent } from 'undici'

import {
  getMissingDownloadDates,
  getNpmChangesLastSeq,
  getNpmPackagesNeedingDailyBackfill,
  getNpmPurlsForChangedNames,
  getNpmUniversePurlsDueForLast30d,
  getUnscannedNpmPurls,
  insertDailyDownloads,
  logAuditFieldChanges,
  markDailyDownloadsProcessed,
  markLast30dProcessed,
  markNpmPackageScanned,
  setNpmChangesLastSeq,
  upsertLast30dDownload,
} from '@crowd/data-access-layer/src/packages'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
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
import { laneCount, proxyForLane, proxyUrl } from './proxies'
import { isFetchError } from './types'
import { upsertPackage } from './upsertPackage'

const log = getServiceChildLogger('npm')

const WORKER = 'npm'

const PURL_NPM_PREFIX = 'pkg:npm/'

// The npm registry name from a purl. Scoped purls are percent-encoded per the purl
// spec (pkg:npm/%40scope/name), so the segment after the prefix must be URL-decoded
// to get the real npm name (@scope/name) used by the registry/downloads APIs.
function npmNameFromPurl(purl: string): string {
  return decodeURIComponent(purl.slice(PURL_NPM_PREFIX.length))
}

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

// 4xx (404 or any other client error like 405 from a malformed/illegal npm name —
// e.g. deps.dev dependency-chain strings that leaked into `packages`). 429 is
// excluded — it's transient and handled by the slow exponential path.
function isClientError(code: number | undefined, kind: string): boolean {
  return kind === 'NOT_FOUND' || (code !== undefined && code >= 400 && code < 500 && code !== 429)
}

// 4xx errors get a few quick in-lane retries with a small linear backoff (1s, 2s),
// then the package is given up on and marked scanned. 429/5xx/network errors are NOT
// handled here — they throw and ride Temporal's exponential activity-retry instead.
const INGEST_4XX_ATTEMPTS = 3
const INGEST_4XX_BACKOFF_MS = 1000

// Fully enrich a single package. `purl` is the source-of-truth identifier from the
// packages row; the npm registry name (for the HTTP fetch) is derived from it.
// `dispatcher` (when present) routes the fetch through this lane's proxy IP.
async function ingestOne(qx: QueryExecutor, purl: string, dispatcher?: Dispatcher): Promise<void> {
  const name = npmNameFromPurl(purl)

  for (let attempt = 1; attempt <= INGEST_4XX_ATTEMPTS; attempt++) {
    const packumentResult = await fetchPackument(name, dispatcher)

    if (!isFetchError(packumentResult)) {
      const { changedFields } = await upsertPackage(qx, packumentResult, purl)
      await logAuditFieldChanges(qx, WORKER, purl, changedFields)
      await markNpmPackageScanned(qx, purl, { status: 'success', attempts: attempt })
      return
    }

    // 429 / 5xx / network → bubble up so Temporal retries the activity with exponential backoff.
    if (!isClientError(packumentResult.statusCode, packumentResult.kind)) {
      throw new Error(`Failed to fetch packument for ${name}: ${packumentResult.message}`)
    }

    // 4xx → quick retry a few times (1s, 2s); give up and skip after the last attempt.
    if (attempt < INGEST_4XX_ATTEMPTS) {
      await sleep(attempt * INGEST_4XX_BACKOFF_MS)
      continue
    }
    log.warn(
      { purl, statusCode: packumentResult.statusCode, kind: packumentResult.kind },
      'npm packument 4xx after fast retries — marking scanned and skipping',
    )
    await markNpmPackageScanned(qx, purl, {
      status: 'error',
      attempts: INGEST_4XX_ATTEMPTS,
      httpStatus: packumentResult.statusCode,
      errorKind: packumentResult.kind,
      message: packumentResult.message,
    })
  }
}

// Number of concurrent lanes shared by all npm workers: one per configured proxy IP
// when the proxy layer is enabled, otherwise a single direct lane.
export async function getLaneCount(): Promise<number> {
  return laneCount()
}

function ingestSleepMs(): number {
  const raw = process.env.CROWD_PACKAGES_INGEST_SLEEP_MS
  const n = parseInt(raw ?? '1200', 10)
  return Number.isFinite(n) && n >= 0 ? n : 1200
}

// One ingest lane: enrich a shard of purls sequentially through a single proxy IP
// (assigned by laneIndex), throttled so the lane stays under the per-IP rate limit.
// One ProxyAgent is reused for the whole shard and closed at the end. 4xx packages
// are skipped inside ingestOne; a transient (429/5xx/network) error throws out of
// here so Temporal retries the lane with exponential backoff.
export async function ingestNpmPackageBatch(purls: string[], laneIndex: number): Promise<void> {
  if (purls.length === 0) return

  const qx = await getPackagesDb()
  const proxy = proxyForLane(laneIndex)
  const dispatcher = proxy ? new ProxyAgent(proxyUrl(proxy)) : undefined

  try {
    for (const purl of purls) {
      await sleep(ingestSleepMs())
      await ingestOne(qx, purl, dispatcher)
    }
  } finally {
    await dispatcher?.close()
  }

  log.info(
    { laneIndex, count: purls.length, exit: proxy?.host ?? 'direct' },
    'Ingested npm package batch',
  )
}

// A bounded, keyset-paginated slice of npm packages in `packages` that have never
// been metadata-scanned. `nextCursor` is the last purl returned (or `afterPurl`
// when empty, so the cursor never regresses).
export async function getUnscannedNpmBatch(
  afterPurl: string,
  batchSize: number,
): Promise<{ purls: string[]; nextCursor: string }> {
  const qx = await getPackagesDb()
  const purls = await getUnscannedNpmPurls(qx, afterPurl, batchSize)
  return { purls, nextCursor: purls.length ? purls[purls.length - 1] : afterPurl }
}

// Filter a _changes feed page down to the purls that exist as npm rows in
// `packages` (purls read straight from those rows, never generated from names).
export async function getChangedNpmPurls(names: string[]): Promise<string[]> {
  if (names.length === 0) return []
  const qx = await getPackagesDb()
  return getNpmPurlsForChangedNames(qx, names)
}

function utcYesterday(): string {
  const d = new Date()
  d.setUTCDate(d.getUTCDate() - 1)
  return d.toISOString().slice(0, 10)
}

export async function currentTimestamp(): Promise<string> {
  return new Date().toISOString()
}

// One daily-downloads lane. Self-selects a disjoint hash-shard of the due packages
// (laneIndex/laneCount) and fetches their missing daily ranges through this lane's
// proxy IP. A transient fetch error throws so Temporal retries the lane; the
// watermark is bumped per package only after its windows are persisted.
export async function backfillDailyLane(
  cutoff: string,
  batchSize: number,
  laneIndex: number,
  lanes: number,
): Promise<{ fetched: number }> {
  const qx = await getPackagesDb()
  const candidates = await getNpmPackagesNeedingDailyBackfill(
    qx,
    cutoff,
    batchSize,
    laneIndex,
    lanes,
  )
  if (candidates.length === 0) return { fetched: 0 }

  const yesterday = utcYesterday()
  const proxy = proxyForLane(laneIndex)
  const dispatcher = proxy ? new ProxyAgent(proxyUrl(proxy)) : undefined

  try {
    for (const pkg of candidates) {
      const lower =
        pkg.firstReleaseAt && pkg.firstReleaseAt.slice(0, 10) > NPM_EARLIEST
          ? pkg.firstReleaseAt.slice(0, 10)
          : NPM_EARLIEST

      if (lower <= yesterday) {
        const missingDates = await getMissingDownloadDates(qx, pkg.id, lower, yesterday)
        for (const w of computeChunks(missingDates)) {
          await sleep(downloadSleepMs())
          const result = await fetchDailyRangeHttp(pkg.name, w.start, w.end, dispatcher)
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

      await markDailyDownloadsProcessed(qx, pkg.purl)
    }
  } finally {
    await dispatcher?.close()
  }

  log.info(
    { laneIndex, count: candidates.length, exit: proxy?.host ?? 'direct' },
    'Backfilled daily downloads lane',
  )
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

// The current rolling 30-day window: end = 1st of this month, start = end − 30 days
// (clamped to NPM_EARLIEST). Same shape computeMissingLast30dWindows emits for its
// last window, so it groups together with new packages' latest window.
function latestLast30dWindow(end: string): Last30dWindow {
  const startDay = new Date(end + 'T00:00:00Z')
  startDay.setUTCDate(startDay.getUTCDate() - 30)
  const start = startDay.toISOString().slice(0, 10)
  return { start: start < NPM_EARLIEST ? NPM_EARLIEST : start, end, isLatest: true }
}

interface Last30dWorkItem {
  purl: string
  name: string // npm registry name (derived from purl)
  window: Last30dWindow
}

// One last-30d lane. Self-selects a disjoint hash-shard of the due packages (per-purl
// watermark + laneIndex/laneCount) and expands each into the windows it needs (new →
// full history, existing → latest only). Fetches grouped BY WINDOW (most recent first)
// so unscoped packages bulk-fetch 128 at a time per window (scoped individually),
// routed through this lane's proxy IP. The watermark is bumped only after every fetch
// in the batch succeeds — an error throws, the activity retries, and nothing is marked.
export async function backfillLast30dLane(
  cutoff: string,
  batchSize: number,
  laneIndex: number,
  lanes: number,
): Promise<{ fetched: number }> {
  const qx = await getPackagesDb()
  const due = await getNpmUniversePurlsDueForLast30d(qx, cutoff, batchSize, laneIndex, lanes)
  if (due.length === 0) return { fetched: 0 }

  const latestEnd = utcFirstOfCurrentMonth()

  const work: Last30dWorkItem[] = []
  for (const c of due) {
    const name = npmNameFromPurl(c.purl)
    const windows = c.isNew
      ? computeMissingLast30dWindows(c.firstReleaseAt, latestEnd, [])
      : [latestLast30dWindow(latestEnd)]
    for (const window of windows) work.push({ purl: c.purl, name, window })
  }

  // Group (purl, window) work by window so each window's unscoped packages bulk.
  const byWindow = new Map<string, Last30dWorkItem[]>()
  for (const item of work) {
    const key = `${item.window.start}:${item.window.end}`
    const arr = byWindow.get(key)
    if (arr) arr.push(item)
    else byWindow.set(key, [item])
  }

  // Process the most recent window first (it carries isLatest, so packages_universe's
  // denormalized downloads_last_30d is refreshed before older history is backfilled),
  // then walk older windows newest→oldest. end is YYYY-MM-DD so a string sort orders.
  const groups = [...byWindow.values()].sort((a, b) =>
    b[0].window.end.localeCompare(a[0].window.end),
  )

  const proxy = proxyForLane(laneIndex)
  const dispatcher = proxy ? new ProxyAgent(proxyUrl(proxy)) : undefined

  try {
    for (const items of groups) {
      const { start, end, isLatest } = items[0].window
      const unscoped = items.filter((i) => !i.name.startsWith('@'))
      const scoped = items.filter((i) => i.name.startsWith('@'))

      for (let i = 0; i < unscoped.length; i += 128) {
        const batch = unscoped.slice(i, i + 128)
        await sleep(downloadSleepMs())
        const result = await fetchBulkPointRange(
          batch.map((b) => b.name),
          start,
          end,
          dispatcher,
        )
        if (isFetchError(result)) {
          throw new Error(
            `Failed to fetch bulk last-30d downloads [${start}:${end}]: ${result.message}`,
          )
        }
        for (const b of batch) {
          const count = result.counts.get(b.name)
          if (count === undefined) {
            log.warn(
              { name: b.name, start, end },
              'npm bulk response: no data for package — skipping',
            )
            continue
          }
          const changedFields = await upsertLast30dDownload(qx, b.purl, start, end, count, isLatest)
          await logAuditFieldChanges(qx, WORKER, b.purl, changedFields)
        }
      }

      for (const s of scoped) {
        await sleep(downloadSleepMs())
        const result = await fetchPointRange(s.name, start, end, dispatcher)
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
          isLatest,
        )
        await logAuditFieldChanges(qx, WORKER, s.purl, changedFields)
      }
    }
  } finally {
    await dispatcher?.close()
  }

  for (const c of due) await markLast30dProcessed(qx, c.purl)

  log.info(
    { laneIndex, count: due.length, exit: proxy?.host ?? 'direct' },
    'Backfilled last-30d downloads lane',
  )
  return { fetched: due.length }
}
