import { getServiceChildLogger } from '@crowd/logging'

import {
  getRubyGemsConfig,
  getRubyGemsCriticalConfig,
  getRubyGemsDependentsConfig,
} from '../config'
import { getPackagesDb } from '../db'

import { processBatch as processCoreBatch } from './runRubyGemsCoreLoop'
import { processBatch as processCriticalBatch } from './runRubyGemsCriticalLoop'
import {
  DependentsBatchResult,
  processBatch as processDependentsBatch,
} from './runRubyGemsDependentsLoop'
import { BatchResult } from './types'

const log = getServiceChildLogger('rubygems-activity')

export async function processRubyGemsCoreBatch(): Promise<BatchResult> {
  const config = getRubyGemsConfig()
  const qx = await getPackagesDb()
  const today = new Date().toISOString().split('T')[0]
  const result = await processCoreBatch(qx, config, today)
  log.info({ ...result }, 'RubyGems core batch complete')
  return result
}

export async function processRubyGemsCriticalBatch(
  afterId = '0',
): Promise<BatchResult & { lastId: string | null }> {
  const config = getRubyGemsCriticalConfig()
  const qx = await getPackagesDb()
  const result = await processCriticalBatch(qx, config, afterId)
  log.info({ ...result }, 'RubyGems critical batch complete')
  return result
}

export async function processRubyGemsDependentsBatch(
  afterId = '0',
): Promise<DependentsBatchResult> {
  const config = getRubyGemsDependentsConfig()
  const qx = await getPackagesDb()
  const result = await processDependentsBatch(qx, config, afterId)
  log.info({ ...result }, 'RubyGems dependents batch complete')
  return result
}
