import { getServiceLogger } from '@crowd/logging'

import { getMavenConfig } from '../config'
import { getPackagesDb } from '../db'
import { runMavenCriticalBackfill } from '../maven/runMavenEnrichmentLoop'

const log = getServiceLogger()

let shuttingDown = false

// Graceful stop: finish the in-flight batch, then exit. Safe to interrupt — every
// write is an idempotent upsert and the DB state is the cursor, so re-running
// resumes where it left off.
const shutdown = () => {
  if (shuttingDown) return
  shuttingDown = true
  log.info('Shutting down maven backfill (stopping after the current batch)...')
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)

const main = async () => {
  log.info('maven backfill starting (one-shot, full extraction)...')

  const config = getMavenConfig()
  log.info(config, 'Config loaded')

  const qx = await getPackagesDb()
  await qx.selectOne('SELECT 1')
  log.info('Connected to packages-db.')

  const totals = await runMavenCriticalBackfill(qx, config, () => shuttingDown)

  log.info({ ...totals }, 'maven backfill complete')
  process.exit(0)
}

main().catch((err) => {
  log.error({ err }, 'maven backfill fatal error')
  process.exit(1)
})
