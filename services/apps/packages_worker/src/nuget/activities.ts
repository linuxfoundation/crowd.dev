import { getServiceChildLogger } from '@crowd/logging'

import { getNuGetConfig } from '../config'
import { getPackagesDb } from '../db'

import { processBatch } from './runNuGetEnrichmentLoop'
import { BatchResult } from './types'

const log = getServiceChildLogger('nuget-activity')

export async function processNuGetBatch(): Promise<BatchResult> {
  const config = getNuGetConfig()
  const qx = await getPackagesDb()

  const today = new Date().toISOString().split('T')[0]
  const result = await processBatch(qx, config, today)
  log.info({ ...result }, 'NuGet batch complete')
  return result
}
