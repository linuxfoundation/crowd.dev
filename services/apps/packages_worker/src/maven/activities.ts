import { getServiceChildLogger } from '@crowd/logging'

import { getMavenConfig } from '../config'
import { getPackagesDb } from '../db'

import { MAVEN_GCS_MIRROR_BASE_URL } from './registry'
import { BatchResult, processBatch } from './runMavenEnrichmentLoop'

const log = getServiceChildLogger('maven-activity')

export async function processMavenCriticalBatch(): Promise<BatchResult> {
  process.env.MAVEN_FETCHER_BASE_URL =
    process.env.MAVEN_FETCHER_BASE_URL_INCREMENTAL ?? MAVEN_GCS_MIRROR_BASE_URL

  const config = getMavenConfig()
  const qx = await getPackagesDb()

  // Universe-polling pass: skip POM extraction when version is unchanged.
  const result = await processBatch(qx, config, true, false)
  log.info({ ...result }, 'Maven critical batch complete')
  return result
}
