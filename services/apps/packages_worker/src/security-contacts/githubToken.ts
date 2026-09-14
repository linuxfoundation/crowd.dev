import { getServiceChildLogger } from '@crowd/logging'

import { getGithubAppConfig } from '../config'
import {
  GithubAppConfig,
  fetchRateLimitDiagnostics,
  getInstallationToken,
  resolveInstallations,
} from '../enricher/githubAppAuth'
import { InstallationPool } from '../enricher/installationPool'

import { GithubGetResult } from './types'

const log = getServiceChildLogger('security-contacts:github-token')

const GITHUB_API = 'https://api.github.com'

// Genuinely-absent / not-determinable → null body, not a failure (see http.ts for the same set).
// 422 covers GitHub's PVR endpoint returning "can't determine" per-repo; 451 is permanent removal.
const ABSENT_STATUSES = new Set([404, 410, 422, 451])

// App-wide ceiling on concurrent requests — GitHub's secondary limit rejects bursts >100/app.
const MAX_CONCURRENT_GITHUB_REQUESTS = 50

const MAX_RATE_LIMIT_RETRIES = 6

/** Minimal async semaphore with fair FIFO hand-off, used to cap concurrent GitHub requests. */
class Semaphore {
  private active = 0
  private readonly waiters: Array<() => void> = []

  constructor(private readonly max: number) {}

  async acquire(): Promise<void> {
    if (this.active < this.max) {
      this.active++
      return
    }
    await new Promise<void>((resolve) => this.waiters.push(resolve))
  }

  release(): void {
    const next = this.waiters.shift()
    if (next) next()
    else this.active--
  }
}

const gate = new Semaphore(MAX_CONCURRENT_GITHUB_REQUESTS)

interface Pool {
  pool: InstallationPool
  appConfig: GithubAppConfig
}

// Module-scoped so installations are resolved once and reused across activity invocations.
let cached: Pool | null = null
let initPromise: Promise<Pool | null> | null = null

async function ensurePool(): Promise<Pool | null> {
  if (cached) return cached
  if (!initPromise) {
    initPromise = (async () => {
      try {
        const appConfig = getGithubAppConfig()
        const discovered = await resolveInstallations(appConfig)
        if (discovered.length === 0) {
          log.warn('No GitHub App installations — authed extractors will run unauthenticated')
          return null
        }
        const healthy = await fetchRateLimitDiagnostics(
          appConfig.appId,
          appConfig.privateKeyPem,
          discovered,
        )
        cached = { pool: new InstallationPool(healthy.length ? healthy : discovered), appConfig }
        return cached
      } catch (err) {
        log.warn(
          { errMsg: (err as Error).message },
          'GitHub token pool unavailable — running unauthenticated',
        )
        return null
      }
    })()
  }
  return initPromise
}

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))

function numOrNull(v: string | null): number | null {
  if (v == null) return null
  const n = parseInt(v, 10)
  return Number.isFinite(n) ? n : null
}

function resetIso(v: string | null): string | null {
  const sec = numOrNull(v)
  return sec == null ? null : new Date(sec * 1000).toISOString()
}

function isRateLimited(status: number, body: string): boolean {
  // Primary limit → 403/429 with x-ratelimit-remaining: 0; secondary → 403/429 mentioning it.
  return status === 429 || (status === 403 && /rate limit|secondary/i.test(body))
}

async function fetchOnce(
  url: string,
  timeoutMs: number,
  headers: Record<string, string>,
): Promise<Response> {
  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs)
  await gate.acquire()
  try {
    return await fetch(url, { headers, signal: controller.signal })
  } finally {
    gate.release()
    clearTimeout(timeoutId)
  }
}

/**
 * Rate-limit-safe GitHub API GET. Rotates across installations in the pool, parking exhausted
 * or rate-limited ones and retrying, up to MAX_RATE_LIMIT_RETRIES. Falls back to a single
 * unauthenticated request when no App is configured.
 */
export async function githubApiGet(
  path: string,
  timeoutMs: number,
  opts: { raw?: boolean; extraOkStatuses?: number[] } = {},
): Promise<GithubGetResult> {
  const accept = opts.raw ? 'application/vnd.github.raw' : 'application/vnd.github+json'
  const okStatuses = opts.extraOkStatuses
    ? new Set([...ABSENT_STATUSES, ...opts.extraOkStatuses])
    : ABSENT_STATUSES
  const url = `${GITHUB_API}${path}`
  const resolved = await ensurePool()

  if (!resolved) {
    const res = await fetchOnce(url, timeoutMs, { Accept: accept })
    if (res.status === 200) return { status: 200, text: await res.text() }
    if (okStatuses.has(res.status)) return { status: res.status, text: null }
    throw new Error(`githubApiGet ${path} failed: HTTP ${res.status}`)
  }

  const { pool, appConfig } = resolved

  for (let attempt = 0; attempt <= MAX_RATE_LIMIT_RETRIES; attempt++) {
    const { installationId, waitMs } = pool.select()
    if (waitMs > 0) {
      log.warn({ waitMs: Math.round(waitMs / 1000) }, 'All installations parked — waiting')
      await sleep(waitMs)
    }

    let token: string
    try {
      token = await getInstallationToken(appConfig.appId, appConfig.privateKeyPem, installationId)
    } catch (err) {
      // Mint failure (rate-limited or auth) — park this installation and try another.
      pool.park(installationId, Date.now() + 60_000)
      if (attempt === MAX_RATE_LIMIT_RETRIES) throw err
      continue
    }

    const res = await fetchOnce(url, timeoutMs, {
      Authorization: `bearer ${token}`,
      Accept: accept,
    })

    if (res.status === 200 || okStatuses.has(res.status)) {
      pool.parkIfBudgetLow(
        installationId,
        numOrNull(res.headers.get('x-ratelimit-remaining')),
        resetIso(res.headers.get('x-ratelimit-reset')),
      )
      return res.status === 200
        ? { status: 200, text: await res.text() }
        : { status: res.status, text: null }
    }

    const body = await res.text().catch(() => '')
    if (isRateLimited(res.status, body)) {
      const retryAfterSec = numOrNull(res.headers.get('retry-after'))
      if (retryAfterSec) {
        // Secondary limits are app-wide, so switching installations won't help — wait it out.
        log.warn({ retryAfterSec }, 'GitHub secondary rate limit — backing off')
        await sleep(retryAfterSec * 1000 + 1_000)
      } else {
        // Primary limit — park this installation until its reset and switch to another.
        const resetSec = numOrNull(res.headers.get('x-ratelimit-reset'))
        pool.park(installationId, resetSec ? resetSec * 1000 + 5_000 : Date.now() + 60_000)
      }
      if (attempt === MAX_RATE_LIMIT_RETRIES) {
        throw new Error(`githubApiGet ${path} rate limited after ${attempt + 1} attempts`)
      }
      continue
    }

    throw new Error(`githubApiGet ${path} failed: HTTP ${res.status}`)
  }

  // Unreachable: the loop either returns or throws on the final attempt.
  throw new Error(`githubApiGet ${path} exhausted retries`)
}
