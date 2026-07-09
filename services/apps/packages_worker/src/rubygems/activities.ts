import { getServiceChildLogger } from '@crowd/logging'

import { getRubyGemsConfig, getRubyGemsCriticalConfig } from '../config'
import { getPackagesDb } from '../db'

import { processBatch as processCoreBatch } from './runRubyGemsCoreLoop'
import { processBatch as processCriticalBatch } from './runRubyGemsCriticalLoop'
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

export async function processRubyGemsCriticalBatch(): Promise<BatchResult> {
  const config = getRubyGemsCriticalConfig()
  const qx = await getPackagesDb()
  const result = await processCriticalBatch(qx, config)
  log.info({ ...result }, 'RubyGems critical batch complete')
  return result
}
