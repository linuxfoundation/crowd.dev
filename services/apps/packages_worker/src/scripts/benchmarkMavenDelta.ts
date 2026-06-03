/**
 * One-shot benchmark for the Maven delta-API sync path.
 *
 * Runs a single processApiChangesBatch() against the configured delta feed and
 * prints a performance summary (fetch vs. process split, throughput, per-package
 * average). Use it to gather numbers before wiring the path into Temporal.
 *
 * Run with env loaded:
 *   pnpm run benchmark:maven-delta:local
 *
 * Requires MAVEN_DELTA_API_URL; set MAVEN_SYNC_SOURCE=api|both so the config
 * validates. Widen MAVEN_DELTA_API_LOOKBACK_MINUTES to pull a bigger window for a
 * more meaningful sample.
 */
import { getServiceLogger } from '@crowd/logging'

import { getMavenConfig } from '../config'
import { getPackagesDb } from '../db'
import { processApiChangesBatch } from '../maven/runMavenEnrichmentLoop'

const log = getServiceLogger()

const main = async () => {
  const config = getMavenConfig()

  if (!config.deltaApi.baseUrl) {
    throw new Error('MAVEN_DELTA_API_URL is required to benchmark the delta-API path')
  }

  log.info(
    {
      baseUrl: config.deltaApi.baseUrl,
      lookbackMinutes: config.deltaApi.lookbackMinutes,
      pageSize: config.deltaApi.pageSize,
      includePrerelease: config.deltaApi.includePrerelease,
      concurrency: config.concurrency,
      groupDelayMs: config.groupDelayMs,
    },
    'Delta-API benchmark starting',
  )

  const qx = await getPackagesDb()
  await qx.selectOne('SELECT 1')

  const startedAt = Date.now()
  const r = await processApiChangesBatch(qx, config)
  const totalMs = Date.now() - startedAt

  const enriched = r.processed + r.skipped + r.error + r.unchanged
  const throughputPerSec = r.processMs > 0 ? +(enriched / (r.processMs / 1000)).toFixed(2) : 0
  const avgMsPerPkg = enriched > 0 ? +(r.processMs / enriched).toFixed(1) : 0

  log.info(
    {
      // window
      apiChanges: r.apiChanges,
      uniquePackages: r.uniquePackages,
      matchedCritical: r.matchedCritical,
      // outcomes
      processed: r.processed,
      skipped: r.skipped,
      unchanged: r.unchanged,
      error: r.error,
      hopLimitReached: r.hopLimitReached,
      // timing
      fetchMs: r.fetchMs,
      processMs: r.processMs,
      totalMs,
      // perf
      throughputPerSec,
      avgMsPerPkg,
    },
    'Delta-API benchmark complete',
  )

  process.exit(0)
}

main().catch((err) => {
  log.error({ err }, 'Delta-API benchmark failed')
  process.exit(1)
})
