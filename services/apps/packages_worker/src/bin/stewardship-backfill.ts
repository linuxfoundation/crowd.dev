import { getServiceLogger } from '@crowd/logging'

import { getPackagesDb } from '../db'
import { runStewardshipBackfill } from '../stewardship/runStewardshipBackfill'

const log = getServiceLogger()

let shuttingDown = false

// Graceful stop: finish the in-flight batch, then exit. Safe to interrupt — every
// write is ON CONFLICT DO NOTHING so re-running resumes where it left off.
const shutdown = () => {
  if (shuttingDown) return
  shuttingDown = true
  log.info('Shutting down stewardship backfill (stopping after the current batch)...')
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)

const main = async () => {
  log.info('stewardship backfill starting...')

  const qx = await getPackagesDb()
  await qx.selectOne('SELECT 1')
  log.info('Connected to packages-db.')

  const rawBatchSize = parseInt(process.env.STEWARDSHIP_BACKFILL_BATCH_SIZE ?? '10000', 10)
  if (!Number.isFinite(rawBatchSize) || rawBatchSize <= 0) {
    throw new Error(
      `STEWARDSHIP_BACKFILL_BATCH_SIZE must be a positive integer, got: ${process.env.STEWARDSHIP_BACKFILL_BATCH_SIZE}`,
    )
  }
  const batchSize = rawBatchSize

  const totals = await runStewardshipBackfill(qx, { batchSize }, () => shuttingDown)

  log.info({ ...totals }, 'stewardship backfill complete')
  process.exit(0)
}

main().catch((err) => {
  log.error({ err }, 'stewardship backfill fatal error')
  process.exit(1)
})
