import { rm } from 'node:fs/promises'
import * as path from 'node:path'

import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

import { deriveCriticalFlag } from './deriveCriticalFlag'
import { fetchEcosystemZip } from './fetchEcosystemZip'
import { parseOsvRecord } from './parseOsvRecord'
import { FetchError, NormalizedRecord, OsvConfig } from './types'
import { upsertAdvisoryBatch } from './upsertAdvisory'

const log = getServiceChildLogger('osv-sync')

const MAX_FETCH_RETRIES = 3

async function withRetry<T>(label: string, fn: () => Promise<T>): Promise<T> {
  let lastErr: Error | undefined
  for (let attempt = 0; attempt <= MAX_FETCH_RETRIES; attempt++) {
    try {
      return await fn()
    } catch (err) {
      lastErr = err as Error
      if (err instanceof FetchError && (err.kind === 'NOT_FOUND' || err.kind === 'PARSE')) {
        throw err
      }
      if (attempt < MAX_FETCH_RETRIES) {
        const backoffMs = 1000 * 2 ** attempt
        log.warn({ label, attempt, backoffMs, err }, 'Transient error, retrying')
        await new Promise((r) => setTimeout(r, backoffMs))
      }
    }
  }
  throw lastErr
}

async function syncEcosystem(
  qx: QueryExecutor,
  config: OsvConfig,
  ecosystem: string,
  allowed: Set<string>,
  isShuttingDown: () => boolean,
): Promise<{ read: number; kept: number; skipped: number; flushed: number }> {
  const ecoDir = path.join(config.tmpDir, ecosystem)
  await rm(ecoDir, { recursive: true, force: true }).catch(() => {
    /* best-effort cleanup; ignore failure */
  })

  // Counters and buffer live inside the retry closure so a transient retry
  // restarts from a clean slate and we don't double-count already-flushed
  // batches. UPSERT is idempotent on osv_id so re-flushing is safe.
  return await withRetry(`fetch ${ecosystem}`, async () => {
    let read = 0
    let kept = 0
    let skipped = 0
    let flushed = 0
    let buffer: NormalizedRecord[] = []

    const flush = async () => {
      if (buffer.length === 0) return
      const batch = buffer
      buffer = []
      await upsertAdvisoryBatch(qx, batch)
      flushed += batch.length
    }

    for await (const entry of fetchEcosystemZip(config.bulkBaseUrl, ecosystem, config.tmpDir)) {
      if (isShuttingDown()) break
      read++
      const normalized = parseOsvRecord(entry.json, allowed)
      if (normalized.packages.length === 0) {
        skipped++
        continue
      }
      kept++
      buffer.push(normalized)
      if (buffer.length >= config.batchSize) {
        await flush()
      }
    }

    await flush()
    return { read, kept, skipped, flushed }
  })
}

export async function runOsvSync(
  qx: QueryExecutor,
  config: OsvConfig,
  isShuttingDown: () => boolean,
): Promise<void> {
  const allowed = new Set(config.ecosystems)

  while (!isShuttingDown()) {
    const passStart = Date.now()
    log.info({ ecosystems: config.ecosystems }, 'Starting OSV sync pass')

    for (const ecosystem of config.ecosystems) {
      if (isShuttingDown()) break
      const ecoStart = Date.now()
      try {
        const stats = await syncEcosystem(qx, config, ecosystem, allowed, isShuttingDown)
        log.info(
          { ecosystem, durationMs: Date.now() - ecoStart, ...stats },
          `OSV sync done for ${ecosystem}`,
        )
      } catch (err) {
        log.error(
          { ecosystem, err },
          `OSV sync failed for ${ecosystem}, continuing with next ecosystem`,
        )
      }
    }

    if (isShuttingDown()) break

    try {
      const { flipped, cleared } = await deriveCriticalFlag(qx, config.deriveBatchSize)
      log.info({ flipped, cleared }, 'deriveCriticalFlag completed')
    } catch (err) {
      log.error({ err }, 'deriveCriticalFlag failed')
    }

    log.info({ durationMs: Date.now() - passStart }, 'OSV sync pass complete')

    // Sleep, but wake up periodically so shutdown is responsive.
    const sleepUntil = Date.now() + config.syncIntervalHours * 3600 * 1000
    while (!isShuttingDown() && Date.now() < sleepUntil) {
      const remainingMs = sleepUntil - Date.now()
      const tickMs = Math.min(config.idleSleepSec * 1000, remainingMs)
      await new Promise((r) => setTimeout(r, tickMs))
    }
  }
}
