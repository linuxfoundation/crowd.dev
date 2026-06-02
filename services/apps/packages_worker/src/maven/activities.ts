import { getServiceChildLogger } from '@crowd/logging'

import { getMavenConfig } from '../config'
import { getPackagesDb } from '../db'
import { BatchResult, processBatch } from './runMavenEnrichmentLoop'

const log = getServiceChildLogger('maven-activity')

export async function processMavenCriticalBatch(): Promise<BatchResult> {
  const config = getMavenConfig()
  const qx = await getPackagesDb()
  const result = await processBatch(qx, config, true)
  log.info({ processed: result.processed, skipped: result.skipped, unchanged: result.unchanged, error: result.error }, 'Maven critical batch complete')
  return result
}

export async function processMavenNonCriticalBatch(): Promise<BatchResult> {
  const config = getMavenConfig()
  const qx = await getPackagesDb()
  const result = await processBatch(qx, config, false)
  log.info({ processed: result.processed, skipped: result.skipped, unchanged: result.unchanged, error: result.error }, 'Maven non-critical batch complete')
  return result
}
