import { ProxyAgent, type Dispatcher } from 'undici'

import {
  getUnscannedPypiPurls,
  logAuditFieldChanges,
  markPypiPackageScanned,
} from '@crowd/data-access-layer/src/packages'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

import { getPackagesDb } from '../db'
import { proxyUrl } from '../proxies'

import { fetchProject } from './fetchProject'
import { pypiNameFromPurl } from './normalize'
import { pypiProxyPool } from './proxies'
import { isFetchError } from './types'
import { upsertProject } from './upsertProject'

const log = getServiceChildLogger('pypi')

const WORKER = 'pypi'

// 4xx (404 or any other client error like a malformed/illegal project name that
// leaked into `packages`). 429 is excluded — it's transient and rides the slow path.
function isClientError(code: number | undefined, kind: string): boolean {
  return kind === 'NOT_FOUND' || (code !== undefined && code >= 400 && code < 500 && code !== 429)
}

// 4xx/malformed get a few quick in-lane retries with a small linear backoff, then the
// package is given up on and marked scanned. 429/5xx/network throw and ride Temporal's
// exponential activity-retry instead.
const INGEST_4XX_ATTEMPTS = 3
const INGEST_4XX_BACKOFF_MS = 1000

// Per-package throttle. PyPI is Fastly-backed and tolerates pip-scale traffic, so a
// modest sleep keeps the lane polite (optional proxy fan-out is off by default).
function ingestSleepMs(): number {
  const n = parseInt(process.env.CROWD_PACKAGES_PYPI_INGEST_SLEEP_MS ?? '300', 10)
  return Number.isFinite(n) && n >= 0 ? n : 300
}

// Re-enrich a critical package whose metadata is older than this many days. PyPI has
// no _changes-style feed, so freshness is staleness-driven.
function refreshDays(): number {
  const n = parseInt(process.env.CROWD_PACKAGES_PYPI_REFRESH_DAYS ?? '14', 10)
  return Number.isFinite(n) && n > 0 ? n : 14
}

// Scope of the metadata sweep. Defaults to true (is_critical packages only — the
// intended steady state). Set CROWD_PACKAGES_PYPI_RUN_ONLY_FOR_CRITICAL=false to enrich
// every PyPI package (temporary, e.g. while criticality is still being populated).
function runOnlyForCritical(): boolean {
  const raw = (process.env.CROWD_PACKAGES_PYPI_RUN_ONLY_FOR_CRITICAL ?? 'true').trim().toLowerCase()
  return !(raw === 'false' || raw === '0' || raw === 'no')
}

// Debug/test switch (CROWD_PACKAGES_PYPI_STOP_AFTER_FIRST_PAGE): when true, the workflow
// processes a single page and returns without continueAsNew
export async function pypiStopAfterFirstPage(): Promise<boolean> {
  const raw = (process.env.CROWD_PACKAGES_PYPI_STOP_AFTER_FIRST_PAGE ?? 'false')
    .trim()
    .toLowerCase()
  return raw === 'true' || raw === '1' || raw === 'yes'
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

// Fully enrich a single package. `purl` is the source-of-truth identifier from the
// packages row; the PyPI project name (for the HTTP fetch) is derived from it.
async function ingestOne(
  qx: QueryExecutor,
  purl: string,
  dispatcher?: Dispatcher,
): Promise<void> {
  const name = pypiNameFromPurl(purl)

  for (let attempt = 1; attempt <= INGEST_4XX_ATTEMPTS; attempt++) {
    const result = await fetchProject(name, dispatcher)

    if (!isFetchError(result)) {
      const { changedFields } = await upsertProject(qx, result, purl)
      await logAuditFieldChanges(qx, WORKER, purl, changedFields)
      await markPypiPackageScanned(qx, purl, { status: 'success', attempts: attempt })
      return
    }

    if (!isClientError(result.statusCode, result.kind) && result.kind !== 'MALFORMED') {
      throw new Error(`Failed to fetch PyPI project ${name}: ${result.message}`)
    }

    if (attempt < INGEST_4XX_ATTEMPTS) {
      await sleep(attempt * INGEST_4XX_BACKOFF_MS)
      continue
    }
    log.warn(
      { purl, statusCode: result.statusCode, kind: result.kind },
      'pypi project 4xx/malformed after fast retries — marking scanned and skipping',
    )
    await markPypiPackageScanned(qx, purl, {
      status: 'error',
      attempts: INGEST_4XX_ATTEMPTS,
      httpStatus: result.statusCode,
      errorKind: result.kind,
      message: result.message,
    })
  }
}

export async function getUnscannedPypiBatch(
  afterPurl: string,
  batchSize: number,
): Promise<{ purls: string[]; nextCursor: string }> {
  const qx = await getPackagesDb()
  const purls = await getUnscannedPypiPurls(
    qx,
    afterPurl,
    batchSize,
    refreshDays(),
    runOnlyForCritical(),
  )
  return { purls, nextCursor: purls.length ? purls[purls.length - 1] : afterPurl }
}

// Enrich a batch of PyPI packages sequentially, throttled to stay polite to the
// registry. 4xx packages are skipped inside ingestOne; a transient (429/5xx/network)
// error throws out of here so Temporal retries the batch with exponential backoff.
export async function ingestPypiPackageBatch(purls: string[]): Promise<void> {
  if (purls.length === 0) return
  const qx = await getPackagesDb()

  // Optional proxy layer (off by default). With a single lane, rotate across the
  // configured proxy pool per package so traffic spreads over all IPs; when disabled the
  // pool is empty and `dispatcher` stays undefined (direct egress). One ProxyAgent per
  // proxy, reused for the whole batch and closed at the end.
  const agents = pypiProxyPool().map((p) => new ProxyAgent(proxyUrl(p)))
  try {
    let i = 0
    for (const purl of purls) {
      await sleep(ingestSleepMs())
      const dispatcher = agents.length ? agents[i++ % agents.length] : undefined
      await ingestOne(qx, purl, dispatcher)
    }
  } finally {
    await Promise.all(agents.map((a) => a.close()))
  }
  log.info({ count: purls.length, proxied: agents.length }, 'Ingested PyPI package batch')
}
