import { ApplicationFailure, Context } from '@temporalio/activity'
import { rm } from 'node:fs/promises'
import * as path from 'node:path'

import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

import { getPackagesDb } from '../db'

import { deriveCriticalFlag } from './deriveCriticalFlag'
import { fetchEcosystemZip } from './fetchEcosystemZip'
import { parseOsvRecord } from './parseOsvRecord'
import { FetchError, NormalizedRecord } from './types'
import { upsertAdvisoryBatch } from './upsertAdvisory'

const log = getServiceChildLogger('osv-sync')

// Read at activity invocation time, not at module load — keeps tests that
// import this file from the sandboxed workflow bundle context working without
// the env being set.
function getActivityConfig() {
  return {
    bulkBaseUrl: required('OSV_BULK_BASE_URL'),
    tmpDir: required('OSV_TMP_DIR'),
    upsertBatchSize: parseInt(required('OSV_BATCH_SIZE'), 10),
    deriveBatchSize: parseInt(required('OSV_DERIVE_BATCH_SIZE'), 10),
  }
}

function required(name: string): string {
  const value = process.env[name]
  if (!value) throw new Error(`Missing required environment variable: ${name}`)
  return value
}

export interface OsvSyncEcosystemInput {
  ecosystem: string
  allowedEcosystems: string[]
}

export interface OsvSyncEcosystemResult {
  ecosystem: string
  read: number
  kept: number
  skipped: number
  flushed: number
  durationMs: number
}

// osvSyncEcosystem downloads <bucket>/<ecosystem>/all.zip, normalizes every
// record, and upserts the surviving advisories into packages-db. The activity
// is idempotent on osv_id, so a Temporal retry that re-runs the activity is
// safe — already-flushed batches simply re-UPSERT with the same values.
//
// Heartbeats fire every 1000 records so Temporal sees the activity is alive
// even on the npm ecosystem (~226k records, ~1 hour with the current N+1
// upsert path tracked under Copilot's deferred review comment).
//
// NOT_FOUND and PARSE FetchErrors are remapped to non-retryable
// ApplicationFailures — the bucket URL is misconfigured or the OSV dataset
// itself is malformed; retrying the same payload won't help. Other FetchError
// kinds (NETWORK, TRANSIENT) propagate so Temporal's RetryPolicy can back off
// and retry.
export async function osvSyncEcosystem(
  input: OsvSyncEcosystemInput,
): Promise<OsvSyncEcosystemResult> {
  const { ecosystem, allowedEcosystems } = input
  const config = getActivityConfig()
  const allowed = new Set(allowedEcosystems)
  const start = Date.now()

  const ecoDir = path.join(config.tmpDir, ecosystem)
  await rm(ecoDir, { recursive: true, force: true }).catch(() => {
    /* best-effort cleanup; ignore failure */
  })

  let read = 0
  let kept = 0
  let skipped = 0
  let flushed = 0
  let buffer: NormalizedRecord[] = []

  const qx = await getPackagesDb()

  const flush = async () => {
    if (buffer.length === 0) return
    const batch = buffer
    buffer = []
    await upsertAdvisoryBatch(qx, batch)
    flushed += batch.length
  }

  try {
    for await (const entry of fetchEcosystemZip(config.bulkBaseUrl, ecosystem, config.tmpDir)) {
      read++
      const normalized = parseOsvRecord(entry.json, allowed)
      if (normalized.packages.length === 0) {
        skipped++
      } else {
        kept++
        buffer.push(normalized)
        if (buffer.length >= config.upsertBatchSize) {
          await flush()
        }
      }
      if (read % 1000 === 0) {
        Context.current().heartbeat({ ecosystem, read, kept, skipped, flushed })
      }
    }

    await flush()
  } catch (err) {
    if (err instanceof FetchError && (err.kind === 'NOT_FOUND' || err.kind === 'PARSE')) {
      throw ApplicationFailure.nonRetryable(
        `OSV sync failed for ${ecosystem}: ${err.message}`,
        err.kind,
      )
    }
    throw err
  }

  const result: OsvSyncEcosystemResult = {
    ecosystem,
    read,
    kept,
    skipped,
    flushed,
    durationMs: Date.now() - start,
  }
  log.info(result, `osvSyncEcosystem done for ${ecosystem}`)
  return result
}

export interface OsvDeriveCriticalFlagResult {
  flipped: number
  cleared: number
  durationMs: number
}

// osvDeriveCriticalFlag recomputes packages.has_critical_vulnerability for
// every package whose latest_version is set. Idempotent — Temporal retry is
// safe; re-running clears stale FALSE→TRUE and TRUE→FALSE transitions
// identically.
export async function osvDeriveCriticalFlag(): Promise<OsvDeriveCriticalFlagResult> {
  const config = getActivityConfig()
  const start = Date.now()
  const qx: QueryExecutor = await getPackagesDb()
  const { flipped, cleared } = await deriveCriticalFlag(qx, config.deriveBatchSize)
  const result = { flipped, cleared, durationMs: Date.now() - start }
  log.info(result, 'osvDeriveCriticalFlag done')
  return result
}
