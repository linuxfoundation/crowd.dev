import { Context } from '@temporalio/activity'
import { type Dispatcher, ProxyAgent } from 'undici'

import {
  DailyDownloadsRunResult,
  Last30dRunResult,
  getExistingLast30dEndDates,
  getMissingDownloadDates,
  getNpmChangesLastSeq,
  getNpmPackagesNeedingDailyBackfill,
  getNpmPurlsForChangedNames,
  getNpmUniversePurlsDueForLast30dHistory,
  getNpmUniversePurlsDueForLatest30d,
  getUnscannedNpmPurls,
  insertDailyDownloads,
  logAuditFieldChanges,
  markDailyDownloadsProcessed,
  markLast30dHistoryBackfilled,
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

      // Outcome recorded on the watermark row. A client error (404/4xx — e.g. a bogus
      // deps.dev dependency-chain name that leaked into `packages`) flips this to error
      // and the package is skipped, not retried. 429/5xx/network still throw below.
      let runResult: DailyDownloadsRunResult = { status: 'success' }

      if (lower <= yesterday) {
        const missingDates = await getMissingDownloadDates(qx, pkg.id, lower, yesterday)
        for (const w of computeChunks(missingDates)) {
          await sleep(downloadSleepMs())
          const result = await fetchDailyRangeHttp(pkg.name, w.start, w.end, dispatcher)
          if (isFetchError(result)) {
            if (!isClientError(result.statusCode, result.kind)) {
              throw new Error(
                `Failed to fetch daily downloads for ${pkg.name} [${w.start}:${w.end}]: ${result.message}`,
              )
            }
            log.warn(
              { purl: pkg.purl, statusCode: result.statusCode, kind: result.kind },
              'npm daily downloads 4xx — marking processed and skipping',
            )
            runResult = {
              status: 'error',
              httpStatus: result.statusCode,
              errorKind: result.kind,
              message: result.message,
            }
            break
          }
          if (result.downloads.length === 0) continue
          const changedFields = await insertDailyDownloads(qx, pkg.id, result.downloads)
          await logAuditFieldChanges(qx, WORKER, pkg.purl, changedFields)
        }
      }

      await markDailyDownloadsProcessed(qx, pkg.purl, runResult)
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
  const n = parseInt(raw ?? '1200', 10)
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
// (clamped to NPM_EARLIEST). isLatest, so its upsert mirrors to packages_universe.
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

interface Last30dPlan {
  purl: string
  name: string // npm registry name (derived from purl)
  windows: Last30dWindow[]
}

async function processLast30dWindowPlans(
  qx: QueryExecutor,
  dispatcher: Dispatcher | undefined,
  plans: Last30dPlan[],
  laneIndex: number,
  onSettled: (purl: string, result: Last30dRunResult) => Promise<void>,
): Promise<void> {
  const work: Last30dWorkItem[] = []
  for (const p of plans) {
    for (const window of p.windows) work.push({ purl: p.purl, name: p.name, window })
  }
  if (work.length === 0) return

  // Group (purl, window) work by window so each window's unscoped packages bulk.
  const byWindow = new Map<string, Last30dWorkItem[]>()
  for (const item of work) {
    const key = `${item.window.start}:${item.window.end}`
    const arr = byWindow.get(key)
    if (arr) arr.push(item)
    else byWindow.set(key, [item])
  }

  // Most recent window first (the latest carries isLatest, so the universe mirror is refreshed
  // before older history), then walk older windows. end is YYYY-MM-DD so a string sort orders.
  const groups = [...byWindow.values()].sort((a, b) =>
    b[0].window.end.localeCompare(a[0].window.end),
  )

  // Per-purl outcome. A client error (404/4xx — e.g. a bogus deps.dev dependency-chain name
  // that leaked into the universe) flips a purl to error; it's then skipped in later windows.
  const results = new Map<string, Last30dRunResult>(
    work.map((w) => [w.purl, { status: 'success' }]),
  )

  // `remaining` counts a purl's outstanding windows in this batch; once it hits 0 (or a client
  // error settles it early) onSettled fires exactly once. The caller decides the DB action.
  const remaining = new Map<string, number>()
  for (const w of work) remaining.set(w.purl, (remaining.get(w.purl) ?? 0) + 1)
  const settled = new Set<string>()

  const settleWindow = async (purl: string): Promise<void> => {
    if (settled.has(purl)) return
    const left = (remaining.get(purl) ?? 1) - 1
    remaining.set(purl, left)
    if (left <= 0) {
      settled.add(purl)
      await onSettled(purl, results.get(purl) ?? { status: 'success' })
    }
  }

  const settleError = async (purl: string, res: Last30dRunResult): Promise<void> => {
    results.set(purl, res)
    if (settled.has(purl)) return
    settled.add(purl)
    await onSettled(purl, res)
  }

  for (const items of groups) {
    const { start, end, isLatest } = items[0].window
    const unscoped = items.filter((i) => !i.name.startsWith('@'))
    const scoped = items.filter((i) => i.name.startsWith('@'))

    for (let i = 0; i < unscoped.length; i += 128) {
      const batch = unscoped
        .slice(i, i + 128)
        .filter((b) => results.get(b.purl)?.status !== 'error')
      if (batch.length === 0) continue
      await sleep(downloadSleepMs())
      const result = await fetchBulkPointRange(
        batch.map((b) => b.name),
        start,
        end,
        dispatcher,
      )
      if (isFetchError(result)) {
        if (!isClientError(result.statusCode, result.kind)) {
          throw new Error(
            `Failed to fetch bulk last-30d downloads [${start}:${end}]: ${result.message}`,
          )
        }
        log.warn(
          { start, end, statusCode: result.statusCode, kind: result.kind, count: batch.length },
          'npm bulk last-30d 4xx — marking batch processed and skipping',
        )
        for (const b of batch) {
          await settleError(b.purl, {
            status: 'error',
            httpStatus: result.statusCode,
            errorKind: result.kind,
            message: result.message,
          })
        }
        continue
      }
      for (const b of batch) {
        const count = result.counts.get(b.name)
        if (count === undefined) {
          log.warn(
            { name: b.name, start, end },
            'npm bulk response: no data for package — skipping',
          )
        } else {
          const changedFields = await upsertLast30dDownload(qx, b.purl, start, end, count, isLatest)
          await logAuditFieldChanges(qx, WORKER, b.purl, changedFields)
        }
        await settleWindow(b.purl)
      }
    }

    for (const s of scoped) {
      // Skip names already known bad (404'd in an earlier, more recent window) so a
      // bogus package isn't re-fetched once per historical window.
      if (results.get(s.purl)?.status === 'error') continue
      await sleep(downloadSleepMs())
      const result = await fetchPointRange(s.name, start, end, dispatcher)
      if (isFetchError(result)) {
        if (!isClientError(result.statusCode, result.kind)) {
          throw new Error(
            `Failed to fetch last-30d downloads for ${s.name} [${start}:${end}]: ${result.message}`,
          )
        }
        log.warn(
          { purl: s.purl, statusCode: result.statusCode, kind: result.kind },
          'npm last-30d 4xx — marking processed and skipping',
        )
        await settleError(s.purl, {
          status: 'error',
          httpStatus: result.statusCode,
          errorKind: result.kind,
          message: result.message,
        })
        continue
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
      await settleWindow(s.purl)
    }

    // Liveness signal after each window so a stalled lane is detected promptly. Combined with
    // the per-round window cap, a retry resumes cheaply — already-stored windows are skipped.
    Context.current().heartbeat({ laneIndex, window: end })
  }
}

// BREADTH lane. Self-selects a disjoint hash-shard of packages due for the current latest
// 30-day window and fetches just that window for each (bulk-128 for unscoped), mirroring the
// count to packages_universe.downloads_last_30d. One window per package keeps the lane fast,
// so the denormalized number lands across the whole universe before any deep history is
// filled. The breadth watermark (downloads_30d_last_run_at) is bumped per package as soon as
// its window is processed — even on a client error — so the monthly run touches each once.
export async function refreshLatestLast30dLane(
  cutoff: string,
  batchSize: number,
  laneIndex: number,
  lanes: number,
): Promise<{ fetched: number }> {
  const qx = await getPackagesDb()
  const due = await getNpmUniversePurlsDueForLatest30d(qx, cutoff, batchSize, laneIndex, lanes)
  if (due.length === 0) return { fetched: 0 }

  const window = latestLast30dWindow(utcFirstOfCurrentMonth())
  const plans: Last30dPlan[] = due.map((c) => ({
    purl: c.purl,
    name: npmNameFromPurl(c.purl),
    windows: [window],
  }))

  const proxy = proxyForLane(laneIndex)
  const dispatcher = proxy ? new ProxyAgent(proxyUrl(proxy)) : undefined

  let marked = 0
  try {
    await processLast30dWindowPlans(qx, dispatcher, plans, laneIndex, async (purl, result) => {
      marked++
      await markLast30dProcessed(qx, purl, result)
    })
  } finally {
    await dispatcher?.close()
  }

  log.info(
    { laneIndex, due: due.length, marked, exit: proxy?.host ?? 'direct' },
    'Refreshed latest last-30d window',
  )
  return { fetched: due.length }
}

// DEPTH lane. Self-selects a disjoint hash-shard of packages whose latest window is already
// present (breadth ran) but whose older history is not yet filled. For each, the still-missing
// older windows are derived from the durable downloads_last_30d rows and capped to the newest
// `windowCap` — a brand-new package has ~140 monthly windows (NPM_EARLIEST → now), so without
// this cap a single activity tried to fetch every window for every package in one shot and
// never finished inside the activity timeout. The latest window is excluded (it's breadth's
// job, so these never re-mirror). The depth watermark (downloads_30d_history_backfilled_at) is
// bumped only once a package's whole backlog drains (or it 4xx's); a partially-backfilled
// package stays unmarked and is re-selected next round until it converges. Because progress
// lives in downloads_last_30d, a timed-out/retried lane skips already-stored windows.
export async function backfillLast30dHistoryLane(
  batchSize: number,
  windowCap: number,
  laneIndex: number,
  lanes: number,
): Promise<{ fetched: number }> {
  const qx = await getPackagesDb()
  const due = await getNpmUniversePurlsDueForLast30dHistory(qx, batchSize, laneIndex, lanes)
  if (due.length === 0) return { fetched: 0 }

  const latestEnd = utcFirstOfCurrentMonth()

  const plans: Last30dPlan[] = []
  const complete = new Map<string, boolean>()
  const settledEmpty: string[] = []
  for (const c of due) {
    const existing = await getExistingLast30dEndDates(qx, c.purl, NPM_EARLIEST, latestEnd)
    // Older windows only — the latest window is the breadth pass's responsibility.
    const missing = computeMissingLast30dWindows(c.firstReleaseAt, latestEnd, existing).filter(
      (w) => !w.isLatest,
    )
    if (missing.length === 0) {
      // No older gaps remain — history is fully filled. Settle the depth watermark so this
      // purl drops out of the history selection.
      settledEmpty.push(c.purl)
      continue
    }
    // computeMissingLast30dWindows returns oldest→newest; take the newest `windowCap`.
    const windows = missing.slice(Math.max(0, missing.length - windowCap))
    complete.set(c.purl, missing.length <= windowCap)
    plans.push({ purl: c.purl, name: npmNameFromPurl(c.purl), windows })
  }

  for (const purl of settledEmpty) {
    await markLast30dHistoryBackfilled(qx, purl, { status: 'success' })
  }

  const proxy = proxyForLane(laneIndex)
  const dispatcher = proxy ? new ProxyAgent(proxyUrl(proxy)) : undefined

  let marked = settledEmpty.length
  try {
    await processLast30dWindowPlans(qx, dispatcher, plans, laneIndex, async (purl, result) => {
      // Stamp the depth watermark only when the purl's whole backlog has drained (or it was
      // given up on via a client error); otherwise leave it for the next round to continue.
      if (result.status === 'error' || complete.get(purl)) {
        marked++
        await markLast30dHistoryBackfilled(qx, purl, result)
      }
    })
  } finally {
    await dispatcher?.close()
  }

  log.info(
    { laneIndex, due: due.length, marked, exit: proxy?.host ?? 'direct' },
    'Backfilled last-30d history lane',
  )
  return { fetched: due.length }
}
