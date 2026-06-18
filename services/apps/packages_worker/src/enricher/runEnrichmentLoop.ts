import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

import { getEnricherConfig } from '../config'

import { fetchActivitySnapshot } from './fetchActivitySnapshot'
import { fetchLightRepo, parseGithubUrl } from './fetchLightRepo'
import { GithubAppConfig, getInstallationToken } from './githubAppAuth'
import { FetchError, LightRepoResult, RepoActivitySnapshot } from './types'
import { bulkUpdateEnrichedRepos, markReposSkipped } from './updateEnrichedRepos'
import { bulkUpsertRepoActivitySnapshot } from './updateRepoActivitySnapshot'

const log = getServiceChildLogger('github-repos-enricher')

const MAX_RETRIES = 3
const DB_FETCH_SIZE = 2000
const WRITE_FLUSH_SIZE = 500
const WRITE_FLUSH_MS = 5000
const MAX_FLUSH_FAILURES = 3
// Park an installation before GitHub starts rejecting — avoids a failed request + requeue
const PROACTIVE_PARK_REMAINING = 50
// Rate-limited snapshots retry once with another installation before being skipped
const SNAPSHOT_RATE_LIMIT_RETRIES = 1
// Installations whose token mint fails (e.g. org IP allowlist) sit out for an hour
const MINT_FAILURE_PARK_MS = 60 * 60 * 1000

// ─── Installation pool ────────────────────────────────────────────────────────

/** Round-robins over installations, skipping ones parked until their rate-limit reset. */
class InstallationPool {
  private readonly parkedUntil = new Map<number, number>()
  private roundRobinIdx = 0

  constructor(private readonly ids: number[]) {}

  select(): { installationId: number; waitMs: number } {
    const now = Date.now()
    const n = this.ids.length

    for (let i = 0; i < n; i++) {
      const idx = (this.roundRobinIdx + i) % n
      const id = this.ids[idx]
      if ((this.parkedUntil.get(id) ?? 0) <= now) {
        this.roundRobinIdx = (idx + 1) % n
        return { installationId: id, waitMs: 0 }
      }
    }

    let soonestReset = Infinity
    let soonestId = this.ids[0]
    for (const id of this.ids) {
      const reset = this.parkedUntil.get(id) ?? 0
      if (reset < soonestReset) {
        soonestReset = reset
        soonestId = id
      }
    }
    return { installationId: soonestId, waitMs: Math.max(1_000, soonestReset - now) }
  }

  park(installationId: number, untilMs: number): void {
    this.parkedUntil.set(installationId, untilMs)
  }

  parkIfBudgetLow(
    installationId: number,
    remaining: number | null | undefined,
    resetAt: string | null | undefined,
  ): void {
    if (remaining == null || resetAt == null || remaining >= PROACTIVE_PARK_REMAINING) return
    this.park(installationId, new Date(resetAt).getTime() + 5_000)
    log.info(
      { installationId, remaining, resetAt },
      'Budget low — proactively parking installation',
    )
  }
}

// ─── Fetch with retries ───────────────────────────────────────────────────────

type FetchOutcome =
  | { kind: 'success'; data: LightRepoResult }
  | { kind: 'permanent' } // NOT_FOUND / AUTH / MALFORMED — mark skip_enrichment
  | { kind: 'transient' } // gave up after retries — leave last_synced_at unset

async function fetchWithRetries(
  url: string,
  token: string,
  timeoutMs: number,
): Promise<FetchOutcome> {
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      return { kind: 'success', data: await fetchLightRepo(url, token, timeoutMs) }
    } catch (err) {
      if (!(err instanceof FetchError)) throw err

      if (['NOT_FOUND', 'AUTH', 'MALFORMED'].includes(err.kind)) {
        log.warn({ url, kind: err.kind }, err.message)
        return { kind: 'permanent' }
      }

      if (err.kind === 'RATE_LIMIT') throw err

      if (attempt < MAX_RETRIES) {
        const backoffMs = 1000 * 2 ** attempt
        log.warn({ url, attempt, backoffMs }, `Transient error, retrying: ${err.message}`)
        await new Promise((r) => setTimeout(r, backoffMs))
      } else {
        log.error({ url }, `Gave up after ${MAX_RETRIES} retries: ${err.message}`)
        return { kind: 'transient' }
      }
    }
  }
  return { kind: 'transient' }
}

// ─── Write buffer ─────────────────────────────────────────────────────────────

class WriteBuffer {
  private results: LightRepoResult[] = []
  private snapshots: RepoActivitySnapshot[] = []
  private skipUrls: string[] = []
  private lastFlushAt = Date.now()
  private flushing = false
  private flushFailures = 0

  constructor(private readonly qx: QueryExecutor) {}

  add(result: LightRepoResult): void {
    this.results.push(result)
  }

  addSnapshot(snapshot: RepoActivitySnapshot): void {
    this.snapshots.push(snapshot)
  }

  addSkip(url: string): void {
    this.skipUrls.push(url)
  }

  shouldFlush(): boolean {
    if (this.flushing) return false
    return (
      this.results.length >= WRITE_FLUSH_SIZE ||
      this.skipUrls.length >= WRITE_FLUSH_SIZE ||
      Date.now() - this.lastFlushAt >= WRITE_FLUSH_MS
    )
  }

  private clearBatch(resultCount: number, snapshotCount: number, skipCount: number): void {
    this.results.splice(0, resultCount)
    this.snapshots.splice(0, snapshotCount)
    this.skipUrls.splice(0, skipCount)
    this.flushFailures = 0
  }

  async flush(): Promise<number> {
    this.lastFlushAt = Date.now()
    if (this.results.length === 0 && this.snapshots.length === 0 && this.skipUrls.length === 0) {
      return 0
    }

    const batch = [...this.results]
    const snapshotBatch = [...this.snapshots]
    const skips = [...this.skipUrls]
    this.flushing = true
    try {
      // The snapshot upsert also updates repos rows — run in one transaction to avoid
      // deadlocking with bulkUpdateEnrichedRepos on overlapping rows (40P01)
      await this.qx.tx(async (tx) => {
        await bulkUpdateEnrichedRepos(tx, batch)
        await markReposSkipped(tx, skips)
        await bulkUpsertRepoActivitySnapshot(tx, snapshotBatch)
      })
      this.clearBatch(batch.length, snapshotBatch.length, skips.length)
      return batch.length
    } catch (err) {
      this.flushFailures++
      // Dropping is safe: the rolled-back repos stay unsynced, so the next sweep retries them
      const dropBatch = this.flushFailures >= MAX_FLUSH_FAILURES
      log.error(
        {
          errCode: (err as { code?: string }).code,
          errName: (err as Error).name,
          errMsg: (err as Error).message,
          errStack: (err as Error).stack,
          flushFailures: this.flushFailures,
          bufferedResults: this.results.length,
          dropBatch,
        },
        dropBatch
          ? 'Flush failed repeatedly — dropping batch, repos will be re-enriched next sweep'
          : 'Flush failed — will retry on next cycle',
      )
      if (dropBatch) this.clearBatch(batch.length, snapshotBatch.length, skips.length)
      return 0
    } finally {
      this.flushing = false
    }
  }
}

// ─── DB cursor stream ─────────────────────────────────────────────────────────

interface RepoRow {
  id: string
  url: string
}

async function fetchDbBatch(
  qx: QueryExecutor,
  cursor: string | null,
  updateIntervalHours: number,
): Promise<RepoRow[]> {
  return qx.select(
    `
    SELECT id, url
    FROM repos
    WHERE host = 'github'
      AND skip_enrichment = false
      AND (last_synced_at IS NULL OR last_synced_at < NOW() - INTERVAL '$(updateIntervalHours) hours')
      AND ($(cursor) IS NULL OR id > $(cursor))
    ORDER BY id
    LIMIT $(dbFetchSize)
    `,
    { cursor, dbFetchSize: DB_FETCH_SIZE, updateIntervalHours },
  )
}

/** Streams repo rows from the DB by cursor, prefetching the next batch as the queue drains. */
class RepoQueue {
  private cursor: string | null = null
  private dbDone = false
  private rows: RepoRow[] = []
  private pendingFetch: Promise<void> | null = null

  constructor(
    private readonly qx: QueryExecutor,
    private readonly updateIntervalHours: number,
  ) {}

  private fill(): void {
    if (this.pendingFetch || this.dbDone) return
    this.pendingFetch = fetchDbBatch(this.qx, this.cursor, this.updateIntervalHours)
      .then((rows) => {
        this.pendingFetch = null
        if (rows.length === 0) {
          this.dbDone = true
        } else {
          this.rows.push(...rows)
          this.cursor = rows[rows.length - 1].id
        }
      })
      .catch((err) => {
        this.pendingFetch = null
        log.warn({ errMsg: (err as Error).message }, 'DB batch fetch failed, will retry')
      })
  }

  async prime(): Promise<void> {
    this.fill()
    await (this.pendingFetch ?? Promise.resolve())
  }

  async next(): Promise<RepoRow | null> {
    while (this.rows.length === 0) {
      if (this.dbDone) return null
      this.fill()
      if (this.pendingFetch) await this.pendingFetch
    }
    if (this.rows.length < DB_FETCH_SIZE / 2 && !this.pendingFetch && !this.dbDone) this.fill()
    return this.rows.shift() ?? null
  }

  requeue(row: RepoRow): void {
    this.rows.unshift(row)
  }

  get depth(): number {
    return this.rows.length
  }
}

// ─── Per-repo pipeline ────────────────────────────────────────────────────────

interface PoolMetrics {
  totalFetched: number
  totalHttpRequests: number
  totalRateLimitCost: number
  startTime: number
}

interface WorkerContext {
  pool: InstallationPool
  queue: RepoQueue
  writeBuffer: WriteBuffer
  appConfig: GithubAppConfig
  config: ReturnType<typeof getEnricherConfig>
  metrics: PoolMetrics
}

/** Fetches the activity snapshot, retrying once with another installation on rate limit. */
async function fetchSnapshotWithRetry(
  row: RepoRow,
  owner: string,
  name: string,
  installationId: number,
  token: string,
  ctx: WorkerContext,
): Promise<void> {
  const { pool, writeBuffer, appConfig, config, metrics } = ctx

  for (let attempt = 0; ; attempt++) {
    try {
      const snapshot = await fetchActivitySnapshot(
        row.id,
        owner,
        name,
        token,
        config.fetchTimeoutMs,
      )
      metrics.totalHttpRequests += snapshot.httpRequestCount
      metrics.totalRateLimitCost += snapshot.rateLimitCost
      writeBuffer.addSnapshot(snapshot)
      pool.parkIfBudgetLow(installationId, snapshot.rateLimitRemaining, snapshot.rateLimitResetAt)
      return
    } catch (err) {
      if (!(err instanceof FetchError) || err.kind !== 'RATE_LIMIT') {
        log.warn(
          {
            url: row.url,
            errKind: err instanceof FetchError ? err.kind : 'UNKNOWN',
            errMsg: (err as Error).message,
          },
          'Snapshot fetch failed — skipping snapshot',
        )
        return
      }

      const resetAt = err.resetAt ?? Date.now() + 60_000
      pool.park(installationId, resetAt)
      const next = pool.select()
      if (attempt >= SNAPSHOT_RATE_LIMIT_RETRIES || next.waitMs > 0) {
        log.warn(
          { installationId, resetAt: new Date(resetAt).toISOString() },
          'Snapshot rate limited — parking installation, skipping snapshot',
        )
        return
      }

      log.warn(
        { installationId, nextId: next.installationId },
        'Snapshot rate limited — parking installation, retrying with another',
      )
      try {
        token = await getInstallationToken(
          appConfig.appId,
          appConfig.privateKeyPem,
          next.installationId,
        )
      } catch (mintErr) {
        if (mintErr instanceof FetchError) {
          const until =
            mintErr.kind === 'RATE_LIMIT'
              ? (mintErr.resetAt ?? Date.now() + 60_000)
              : Date.now() + MINT_FAILURE_PARK_MS
          pool.park(next.installationId, until)
          log.warn(
            { installationId: next.installationId, errMsg: mintErr.message },
            'Token mint failed during snapshot retry — skipping snapshot',
          )
          return
        }
        throw mintErr
      }
      installationId = next.installationId
    }
  }
}

/** Enriches one repo: light fetch + activity snapshot, with rate-limit parking/requeue. */
async function processRepo(row: RepoRow, ctx: WorkerContext): Promise<void> {
  const { pool, queue, writeBuffer, appConfig, config, metrics } = ctx

  let owner: string
  let name: string
  try {
    ;({ owner, name } = parseGithubUrl(row.url))
  } catch {
    log.warn({ url: row.url }, 'Skipping non-GitHub URL')
    writeBuffer.addSkip(row.url)
    return
  }

  const { installationId, waitMs } = pool.select()
  if (waitMs > 0) {
    log.warn(
      { waitMs: Math.round(waitMs / 1000) },
      `All installations parked, waiting ${Math.round(waitMs / 1000)}s`,
    )
    await new Promise((r) => setTimeout(r, waitMs))
  }

  try {
    const token = await getInstallationToken(
      appConfig.appId,
      appConfig.privateKeyPem,
      installationId,
    )
    metrics.totalHttpRequests++
    const outcome = await fetchWithRetries(row.url, token, config.fetchTimeoutMs)

    if (outcome.kind === 'success') {
      metrics.totalFetched++
      writeBuffer.add(outcome.data)
      pool.parkIfBudgetLow(
        installationId,
        outcome.data.rateLimit?.remaining,
        outcome.data.rateLimit?.resetAt,
      )
      await fetchSnapshotWithRetry(row, owner, name, installationId, token, ctx)
    } else if (outcome.kind === 'permanent') {
      writeBuffer.addSkip(row.url)
    }
  } catch (err) {
    if (err instanceof FetchError && err.kind === 'RATE_LIMIT') {
      const resetAt = err.resetAt ?? Date.now() + 60_000
      pool.park(installationId, resetAt)
      log.warn(
        { installationId, resetAt: new Date(resetAt).toISOString() },
        'Installation rate limited — parking, re-queuing url',
      )
      queue.requeue(row)
    } else if (err instanceof FetchError && err.kind === 'AUTH') {
      // Only token minting throws AUTH here — repo-level AUTH is handled in fetchWithRetries
      pool.park(installationId, Date.now() + MINT_FAILURE_PARK_MS)
      log.warn(
        { installationId, errMsg: err.message },
        'Token mint failed — parking installation for 1h, re-queuing url',
      )
      queue.requeue(row)
    } else {
      log.error(
        {
          url: row.url,
          errName: (err as Error).name,
          errMsg: (err as Error).message,
          errStack: (err as Error).stack,
        },
        'Unexpected error while enriching repo',
      )
    }
  }
}

// ─── Streaming pool ───────────────────────────────────────────────────────────

async function runStreamingPool(
  qx: QueryExecutor,
  installationIds: number[],
  appConfig: GithubAppConfig,
  config: ReturnType<typeof getEnricherConfig>,
  isShuttingDown: () => boolean,
  metrics: PoolMetrics,
): Promise<'exhausted' | 'shutdown'> {
  const ctx: WorkerContext = {
    pool: new InstallationPool(installationIds),
    queue: new RepoQueue(qx, config.updateIntervalHours),
    writeBuffer: new WriteBuffer(qx),
    appConfig,
    config,
    metrics,
  }
  let logTimer = Date.now()

  await ctx.queue.prime()

  await Promise.all(
    Array.from({ length: config.concurrency }, async () => {
      while (!isShuttingDown()) {
        const row = await ctx.queue.next()
        if (!row) break

        await processRepo(row, ctx)

        if (ctx.writeBuffer.shouldFlush()) {
          const flushed = await ctx.writeBuffer.flush()
          if (Date.now() - logTimer >= 10_000) {
            logTimer = Date.now()
            const elapsedHours = (Date.now() - metrics.startTime) / 3_600_000
            log.info(
              {
                totalFetched: metrics.totalFetched,
                totalHttpRequests: metrics.totalHttpRequests,
                totalRateLimitCost: metrics.totalRateLimitCost,
                reposPerHour:
                  elapsedHours > 0
                    ? Math.round(metrics.totalFetched / elapsedHours)
                    : metrics.totalFetched,
                httpReqsPerHour:
                  elapsedHours > 0
                    ? Math.round(metrics.totalHttpRequests / elapsedHours)
                    : metrics.totalHttpRequests,
                flushed,
                queueDepth: ctx.queue.depth,
              },
              'Throughput snapshot',
            )
          }
        }
      }
    }),
  )

  await ctx.writeBuffer.flush()
  return isShuttingDown() ? 'shutdown' : 'exhausted'
}

// ─── Public entry point ───────────────────────────────────────────────────────

export async function runEnrichmentLoop(
  qx: QueryExecutor,
  installationIds: number[],
  appConfig: GithubAppConfig,
  config: ReturnType<typeof getEnricherConfig>,
  isShuttingDown: () => boolean,
): Promise<void> {
  const metrics = {
    totalFetched: 0,
    totalHttpRequests: 0,
    totalRateLimitCost: 0,
    startTime: Date.now(),
  }

  while (!isShuttingDown()) {
    const outcome = await runStreamingPool(
      qx,
      installationIds,
      appConfig,
      config,
      isShuttingDown,
      metrics,
    )

    if (outcome === 'shutdown') break

    log.info(
      {
        totalFetched: metrics.totalFetched,
        totalHttpRequests: metrics.totalHttpRequests,
        totalRateLimitCost: metrics.totalRateLimitCost,
      },
      `All repos processed — sleeping ${config.idleSleepSec}s`,
    )
    await new Promise((r) => setTimeout(r, config.idleSleepSec * 1000))
  }

  log.info('Enrichment loop stopped')
}
