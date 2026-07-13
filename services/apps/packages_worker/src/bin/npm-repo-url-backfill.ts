import { getServiceLogger } from '@crowd/logging'

import { getPackagesDb } from '../db'
import { backfillNpmRepositoryUrls } from '../npm/backfillRepositoryUrl'

const log = getServiceLogger()

let shuttingDown = false

// Graceful stop: finish the in-flight batch, then exit. Safe to interrupt — every
// write is an idempotent UPDATE recomputed from declared_repository_url, so
// re-running simply reprocesses and skips rows that already match.
const shutdown = () => {
  if (shuttingDown) return
  shuttingDown = true
  log.info('Shutting down npm repo-url backfill (stopping after the current batch)...')
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)

const DEFAULT_BATCH_SIZE = 5000

const main = async () => {
  const dryRun = process.argv.includes('--dry-run')
  const criticalOnly = process.argv.includes('--critical-only')
  const rawBatchSize = process.env.NPM_REPO_URL_BACKFILL_BATCH_SIZE
  const parsedBatchSize = rawBatchSize === undefined ? DEFAULT_BATCH_SIZE : Number(rawBatchSize)
  if (!Number.isInteger(parsedBatchSize) || parsedBatchSize <= 0) {
    log.error(
      { NPM_REPO_URL_BACKFILL_BATCH_SIZE: rawBatchSize },
      'NPM_REPO_URL_BACKFILL_BATCH_SIZE must be a positive integer',
    )
    process.exit(1)
  }
  const batchSize = parsedBatchSize

  log.info(
    { dryRun, criticalOnly, batchSize },
    'npm repo-url backfill starting (recompute canonicalizeRepoUrl from declared_repository_url, no packument fetch)...',
  )

  const qx = await getPackagesDb()
  await qx.selectOne('SELECT 1')
  log.info('Connected to packages-db.')

  const totals = await backfillNpmRepositoryUrls(qx, {
    batchSize,
    dryRun,
    criticalOnly,
    isShuttingDown: () => shuttingDown,
  })

  log.info({ ...totals, dryRun }, 'npm repo-url backfill complete')
  process.exit(0)
}

main().catch((err) => {
  log.error({ err }, 'npm repo-url backfill fatal error')
  process.exit(1)
})
