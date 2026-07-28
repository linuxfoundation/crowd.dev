import { cancellationSignal, heartbeat } from '@temporalio/activity'

import { getServiceChildLogger } from '@crowd/logging'

import { getRubyGemsConfig, getRubyGemsCriticalConfig } from '../config'
import { getPackagesDb } from '../db'

import { processBatch as processCoreBatch } from './runRubyGemsCoreLoop'
import { processBatch as processCriticalBatch } from './runRubyGemsCriticalLoop'
import { BatchResult } from './types'

const log = getServiceChildLogger('rubygems-activity')

// Fixed-cadence heartbeat: a concurrency group can outlast the 2-minute heartbeatTimeout
// (Retry-After sleeps, slow persistence), so heartbeating can't rely on group boundaries.
function startHeartbeat(): () => void {
  const timer = setInterval(() => {
    try {
      heartbeat()
    } catch (err) {
      log.warn({ errMsg: (err as Error).message }, 'Heartbeat failed')
    }
  }, 30_000)
  return () => clearInterval(timer)
}

export async function processRubyGemsCoreBatch(): Promise<BatchResult> {
  const config = getRubyGemsConfig()
  const qx = await getPackagesDb()
  const today = new Date().toISOString().split('T')[0]
  const stopHeartbeat = startHeartbeat()
  try {
    const result = await processCoreBatch(qx, config, today, cancellationSignal())
    log.info({ ...result }, 'RubyGems core batch complete')
    return result
  } finally {
    stopHeartbeat()
  }
}

export async function processRubyGemsCriticalBatch(
  afterId = '0',
): Promise<BatchResult & { lastId: string | null }> {
  const config = getRubyGemsCriticalConfig()
  const qx = await getPackagesDb()
  const stopHeartbeat = startHeartbeat()
  try {
    const result = await processCriticalBatch(qx, config, afterId, cancellationSignal())
    log.info({ ...result }, 'RubyGems critical batch complete')
    return result
  } finally {
    stopHeartbeat()
  }
}
