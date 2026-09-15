import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'

import { runStarSnapshotBackfill } from '../backfill/starSnapshotBackfill'

const log = getServiceLogger()

let shuttingDown = false

// Graceful stop: finish the in-flight batch, then exit. Safe to interrupt and re-run —
// `--after-url` resumes from where it left off, and every write is an idempotent upsert.
const shutdown = () => {
  if (shuttingDown) return
  shuttingDown = true
  log.info('Shutting down star snapshot backfill (stopping after the current batch)...')
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)

const DEFAULT_RESERVED_CORE_RATE_LIMIT = 2_000
const DEFAULT_CONCURRENCY = 5

function readFlagValue(flag: string): string | undefined {
  const index = process.argv.indexOf(flag)
  return index === -1 ? undefined : process.argv[index + 1]
}

function readIntEnv(name: string, defaultValue: number, allowZero: boolean): number {
  const raw = process.env[name]
  const parsed = raw === undefined ? defaultValue : Number(raw)
  if (!Number.isInteger(parsed) || (allowZero ? parsed < 0 : parsed <= 0)) {
    log.error(
      { [name]: raw },
      `${name} must be a${allowZero ? ' non-negative' : ' positive'} integer`,
    )
    process.exit(1)
  }
  return parsed
}

const main = async () => {
  const dryRun = process.argv.includes('--dry-run')
  const afterUrl = readFlagValue('--after-url')
  const reservedCoreRateLimit = readIntEnv(
    'STAR_SNAPSHOT_BACKFILL_RESERVED_CORE_RATE_LIMIT',
    DEFAULT_RESERVED_CORE_RATE_LIMIT,
    true,
  )
  const concurrency = readIntEnv('STAR_SNAPSHOT_BACKFILL_CONCURRENCY', DEFAULT_CONCURRENCY, false)

  log.info(
    { dryRun, afterUrl, reservedCoreRateLimit, concurrency },
    'star snapshot backfill starting (backfilling repositoryStarSnapshots from GitHub stargazers/history)...',
  )

  const conn = await getDbConnection(WRITE_DB_CONFIG())
  const qx = pgpQx(conn)
  await qx.selectOne('SELECT 1')
  log.info('Connected to database.')

  const totals = await runStarSnapshotBackfill(qx, log, {
    reservedCoreRateLimit,
    concurrency,
    dryRun,
    afterUrl,
    isShuttingDown: () => shuttingDown,
  })

  log.info({ ...totals, dryRun }, 'star snapshot backfill complete')
  process.exit(0)
}

main().catch((err) => {
  log.error({ err }, 'star snapshot backfill fatal error')
  process.exit(1)
})
