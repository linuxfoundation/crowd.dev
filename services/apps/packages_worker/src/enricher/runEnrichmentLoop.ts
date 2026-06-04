import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

import { getEnricherConfig } from '../config'

import { fetchLightRepo, parseGithubUrl } from './fetchLightRepo'
import { GithubAppConfig, getInstallationToken } from './githubAppAuth'
import { FetchError, LightRepoResult } from './types'
import { bulkUpdateEnrichedRepos, markReposSkipped } from './updateEnrichedRepos'

const log = getServiceChildLogger('github-repos-enricher')

const MAX_RETRIES = 3
const DB_FETCH_SIZE = 2000
const WRITE_FLUSH_SIZE = 500
const WRITE_FLUSH_MS = 5000

// ─── Token selection ─────────────────────────────────────────────────────────

function selectInstallation(
  installationIds: number[],
  parkedUntil: Map<number, number>,
  roundRobinIdx: { value: number },
): { installationId: number; waitMs: number } {
  const now = Date.now()
  const n = installationIds.length

  for (let i = 0; i < n; i++) {
    const idx = (roundRobinIdx.value + i) % n
    const id = installationIds[idx]
    if ((parkedUntil.get(id) ?? 0) <= now) {
      roundRobinIdx.value = (idx + 1) % n
      return { installationId: id, waitMs: 0 }
    }
  }

  let soonestReset = Infinity
  let soonestId = installationIds[0]
  for (const id of installationIds) {
    const reset = parkedUntil.get(id) ?? 0
    if (reset < soonestReset) {
      soonestReset = reset
      soonestId = id
    }
  }
  return { installationId: soonestId, waitMs: Math.max(1_000, soonestReset - now) }
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
  private skipUrls: string[] = []
  private lastFlushAt = Date.now()
  private flushing = false

  constructor(private readonly qx: QueryExecutor) {}

  add(result: LightRepoResult): void {
    this.results.push(result)
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

  async flush(): Promise<number> {
    const batch = [...this.results]
    const skips = [...this.skipUrls]
    this.lastFlushAt = Date.now()
    this.flushing = true
    try {
      await Promise.all([bulkUpdateEnrichedRepos(this.qx, batch), markReposSkipped(this.qx, skips)])
      // Clear only after both writes succeed — preserves items if the DB call throws
      this.results.splice(0, batch.length)
      this.skipUrls.splice(0, skips.length)
    } finally {
      this.flushing = false
    }
    return batch.length
  }
}

// ─── DB cursor stream ─────────────────────────────────────────────────────────

async function fetchDbBatch(
  qx: QueryExecutor,
  cursor: string | null,
  updateIntervalHours: number,
): Promise<Array<{ id: string; url: string }>> {
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

// ─── Streaming pool ───────────────────────────────────────────────────────────

async function runStreamingPool(
  qx: QueryExecutor,
  installationIds: number[],
  appConfig: GithubAppConfig,
  config: ReturnType<typeof getEnricherConfig>,
  isShuttingDown: () => boolean,
  metrics: { totalFetched: number; totalRequests: number; startTime: number },
): Promise<'exhausted' | 'shutdown'> {
  const parkedUntil = new Map<number, number>()
  const roundRobinIdx = { value: 0 }
  const writeBuffer = new WriteBuffer(qx)

  let cursor: string | null = null
  let dbDone = false
  const queue: Array<{ id: string; url: string }> = []
  let pendingFetch: Promise<void> | null = null
  let logTimer = Date.now()

  const fillQueue = (): void => {
    if (pendingFetch || dbDone) return
    pendingFetch = fetchDbBatch(qx, cursor, config.updateIntervalHours)
      .then((rows) => {
        pendingFetch = null
        if (rows.length === 0) {
          dbDone = true
        } else {
          queue.push(...rows)
          cursor = rows[rows.length - 1].id
        }
      })
      .catch((err) => {
        pendingFetch = null
        log.warn({ err }, 'DB batch fetch failed, will retry')
      })
  }

  const nextRow = async (): Promise<{ id: string; url: string } | null> => {
    while (queue.length === 0) {
      if (dbDone) return null
      fillQueue()
      if (pendingFetch) await pendingFetch
    }
    // Prefetch next batch when queue gets low
    if (queue.length < DB_FETCH_SIZE / 2 && !pendingFetch && !dbDone) fillQueue()
    return queue.shift() ?? null
  }

  // Prime the queue before workers start
  fillQueue()
  await (pendingFetch ?? Promise.resolve())

  await Promise.all(
    Array.from({ length: config.concurrency }, async () => {
      while (!isShuttingDown()) {
        const row = await nextRow()
        if (!row) break

        try {
          parseGithubUrl(row.url)
        } catch {
          log.warn({ url: row.url }, 'Skipping non-GitHub URL')
          writeBuffer.addSkip(row.url)
          continue
        }

        const { installationId, waitMs } = selectInstallation(
          installationIds,
          parkedUntil,
          roundRobinIdx,
        )

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
          metrics.totalRequests++
          const outcome = await fetchWithRetries(row.url, token, config.fetchTimeoutMs)

          if (outcome.kind === 'success') {
            metrics.totalFetched++
            writeBuffer.add(outcome.data)
          } else if (outcome.kind === 'permanent') {
            writeBuffer.addSkip(row.url)
          }
        } catch (err) {
          if (err instanceof FetchError && err.kind === 'RATE_LIMIT') {
            const resetAt = err.resetAt ?? Date.now() + 60_000
            parkedUntil.set(installationId, resetAt)
            log.warn(
              { installationId, resetAt: new Date(resetAt).toISOString() },
              'Installation rate limited — parking, re-queuing url',
            )
            queue.unshift(row)
          } else {
            log.error({ url: row.url, err }, 'Unexpected error')
          }
        }

        if (writeBuffer.shouldFlush()) {
          const flushed = await writeBuffer.flush()
          if (Date.now() - logTimer >= 10_000) {
            logTimer = Date.now()
            const elapsedHours = (Date.now() - metrics.startTime) / 3_600_000
            log.info(
              {
                totalFetched: metrics.totalFetched,
                totalRequests: metrics.totalRequests,
                reposPerHour:
                  elapsedHours > 0
                    ? Math.round(metrics.totalFetched / elapsedHours)
                    : metrics.totalFetched,
                reqsPerHour:
                  elapsedHours > 0
                    ? Math.round(metrics.totalRequests / elapsedHours)
                    : metrics.totalRequests,
                flushed,
                queueDepth: queue.length,
              },
              'Throughput snapshot',
            )
          }
        }
      }
    }),
  )

  await writeBuffer.flush()
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
  const metrics = { totalFetched: 0, totalRequests: 0, startTime: Date.now() }

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
      { totalFetched: metrics.totalFetched, totalRequests: metrics.totalRequests },
      `All repos processed — sleeping ${config.idleSleepSec}s`,
    )
    await new Promise((r) => setTimeout(r, config.idleSleepSec * 1000))
  }

  log.info('Enrichment loop stopped')
}
