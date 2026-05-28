import { getServiceChildLogger } from '@crowd/logging'

import { getPomFetcherConfig } from '../config'
import { getPackagesDb } from '../db'
import { BatchResult, processBatch } from './runPomEnrichmentLoop'

const log = getServiceChildLogger('pom-fetcher-activity')

export async function processMavenCriticalBatch(): Promise<BatchResult> {
  const config = getPomFetcherConfig()
  const qx = await getPackagesDb()
  const result = await processBatch(qx, config, true)
  log.info({ processed: result.processed, skipped: result.skipped, errors: result.errors }, 'Maven critical batch complete')
  return result
}

export async function processMavenNonCriticalBatch(): Promise<BatchResult> {
  const config = getPomFetcherConfig()
  const qx = await getPackagesDb()
  const result = await processBatch(qx, config, false)
  log.info({ processed: result.processed, skipped: result.skipped, errors: result.errors }, 'Maven non-critical batch complete')
  return result
}
