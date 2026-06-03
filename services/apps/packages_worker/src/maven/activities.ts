import { getServiceChildLogger } from '@crowd/logging'

import { getMavenConfig } from '../config'
import { getPackagesDb } from '../db'

import { BatchResult, processApiChangesBatch, processBatch } from './runMavenEnrichmentLoop'

const log = getServiceChildLogger('maven-activity')

function addBatchResult(into: BatchResult, from: BatchResult): void {
  into.processed += from.processed
  into.skipped += from.skipped
  into.error += from.error
  into.unchanged += from.unchanged
  into.hopLimitReached += from.hopLimitReached
}

export async function processMavenCriticalBatch(): Promise<BatchResult> {
  const config = getMavenConfig()
  const qx = await getPackagesDb()

  const total: BatchResult = {
    processed: 0,
    skipped: 0,
    error: 0,
    unchanged: 0,
    hopLimitReached: 0,
  }

  // Delta-API pass: enrich what our feed reports as changed (forces full extraction).
  if (config.syncSource === 'api' || config.syncSource === 'both') {
    try {
      const apiResult = await processApiChangesBatch(qx, config)
      log.info({ ...apiResult }, 'Maven delta-API batch complete')
      addBatchResult(total, apiResult)
    } catch (err) {
      // In 'both' mode the universe-polling pass is the reliable backbone, so a flaky
      // delta feed must never block it — log and continue. In 'api' mode there is no
      // fallback, so let the activity fail and have Temporal retry it.
      if (config.syncSource === 'api') throw err
      const message = err instanceof Error ? err.message : String(err)
      log.warn({ error: message }, 'Delta-API pass failed — continuing with universe-polling pass')
    }
  }

  // Universe-polling pass: current behaviour — skip POM extraction when version is unchanged.
  if (config.syncSource === 'maven' || config.syncSource === 'both') {
    const mavenResult = await processBatch(qx, config, true, false)
    log.info({ ...mavenResult }, 'Maven critical batch complete')
    addBatchResult(total, mavenResult)
  }

  return total
}

export async function processMavenNonCriticalBatch(): Promise<BatchResult> {
  const config = getMavenConfig()
  const qx = await getPackagesDb()
  // Non-critical is DB-only (no POM fetch); the flag is unused on this path.
  const result = await processBatch(qx, config, false, false)
  log.info(
    {
      processed: result.processed,
      skipped: result.skipped,
      unchanged: result.unchanged,
      error: result.error,
    },
    'Maven non-critical batch complete',
  )
  return result
}
