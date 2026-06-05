import { getServiceChildLogger } from '@crowd/logging'

import { getMavenConfig } from '../config'
import { getPackagesDb } from '../db'

import { BatchResult, processBatch } from './runMavenEnrichmentLoop'

const log = getServiceChildLogger('maven-activity')

export async function processMavenCriticalBatch(): Promise<BatchResult> {
  process.env.MAVEN_FETCHER_BASE_URL =
    process.env.MAVEN_FETCHER_BASE_URL_INCREMENTAL ?? 'https://repo1.maven.org/maven2'

  const config = getMavenConfig()
  const qx = await getPackagesDb()

  // Universe-polling pass: skip POM extraction when version is unchanged.
  const result = await processBatch(qx, config, true, false)
  log.info({ ...result }, 'Maven critical batch complete')
  return result
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
