import { getServiceLogger } from '@crowd/logging'

import { getPackagesDb } from '../db'
import { backfillMavenRepositoryUrls } from '../maven/backfillRepositoryUrl'

const log = getServiceLogger()

let shuttingDown = false

// Graceful stop: finish the in-flight batch, then exit. Safe to interrupt — every
// write is an idempotent UPDATE recomputed from declared_repository_url, so
// re-running simply reprocesses and skips rows that already match.
const shutdown = () => {
  if (shuttingDown) return
  shuttingDown = true
  log.info('Shutting down maven repo-url backfill (stopping after the current batch)...')
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)

const DEFAULT_BATCH_SIZE = 5000

const main = async () => {
  const dryRun = process.argv.includes('--dry-run')
  const rawBatchSize = process.env.MAVEN_REPO_URL_BACKFILL_BATCH_SIZE
  const parsedBatchSize = rawBatchSize === undefined ? DEFAULT_BATCH_SIZE : Number(rawBatchSize)
  if (!Number.isInteger(parsedBatchSize) || parsedBatchSize <= 0) {
    log.error(
      { MAVEN_REPO_URL_BACKFILL_BATCH_SIZE: rawBatchSize },
      'MAVEN_REPO_URL_BACKFILL_BATCH_SIZE must be a positive integer',
    )
    process.exit(1)
  }
  const batchSize = parsedBatchSize

  log.info(
    { dryRun, batchSize },
    'maven repo-url backfill starting (recompute normalizeScmUrl from declared_repository_url, no POM fetch)...',
  )

  const qx = await getPackagesDb()
  await qx.selectOne('SELECT 1')
  log.info('Connected to packages-db.')

  const totals = await backfillMavenRepositoryUrls(qx, {
    batchSize,
    dryRun,
    isShuttingDown: () => shuttingDown,
  })

  log.info({ ...totals, dryRun }, 'maven repo-url backfill complete')
  process.exit(0)
}

main().catch((err) => {
  log.error({ err }, 'maven repo-url backfill fatal error')
  process.exit(1)
})
