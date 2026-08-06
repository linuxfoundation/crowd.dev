import { getServiceLogger } from '@crowd/logging'

import { getMavenConfig } from '../config'
import { getPackagesDb } from '../db'
import {
  runMavenCriticalBackfill,
  runMavenCriticalForceBackfill,
} from '../maven/runMavenEnrichmentLoop'

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
  process.env.MAVEN_FETCHER_BASE_URL =
    process.env.MAVEN_FETCHER_BASE_URL_BACKFILL ??
    'https://maven-central.storage-download.googleapis.com/maven2'

  // --force: re-run POM extraction over EVERY critical row, ignoring the
  // staleness window. Use to fully re-apply extraction changes (e.g. SCM
  // interpolation) after the queue has already drained. The default path only
  // picks rows due by refreshDays and cannot be coaxed into a full pass by
  // setting refreshDays=0 (that reprocesses the first batch forever).
  const force = process.argv.includes('--force')

  log.info({ force }, 'maven backfill starting (one-shot, full extraction)...')

  const config = getMavenConfig()
  log.info(config, 'Config loaded')

  const qx = await getPackagesDb()
  await qx.selectOne('SELECT 1')
  log.info('Connected to packages-db.')

  const totals = force
    ? await runMavenCriticalForceBackfill(qx, config, () => shuttingDown)
    : await runMavenCriticalBackfill(qx, config, () => shuttingDown)

  log.info({ ...totals }, 'maven backfill complete')
  process.exit(0)
}

main().catch((err) => {
  log.error({ err }, 'maven backfill fatal error')
  process.exit(1)
})
