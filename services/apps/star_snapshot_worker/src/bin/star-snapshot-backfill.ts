import { randomUUID } from 'crypto'
import { mkdir, readFile, rename, rm, writeFile } from 'fs/promises'
import { dirname } from 'path'

import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'

import { runStarSnapshotBackfill } from '../backfill/starSnapshotBackfill'

const log = getServiceLogger()

let shuttingDown = false

// Graceful stop: finish the in-flight batch, then exit. Safe to interrupt and re-run —
// the checkpoint file (or --after-url) resumes from where it left off, and every write
// is an idempotent upsert.
const shutdown = () => {
  if (shuttingDown) return
  shuttingDown = true
  log.info('Shutting down star snapshot backfill (stopping after the current batch)...')
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)

const DEFAULT_RESERVED_CORE_RATE_LIMIT = 2_000
const DEFAULT_CONCURRENCY = 5
const DEFAULT_CHECKPOINT_FILE = '/var/lib/star-snapshot-worker/backfill-checkpoint.json'
const DEFAULT_COMPLETED_REPOS_FILE = '/var/lib/star-snapshot-worker/backfill-completed-repos.json'

interface Checkpoint {
  afterUrl: string
  updatedAt: string
}

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

async function readCheckpoint(path: string): Promise<Checkpoint | undefined> {
  try {
    return JSON.parse(await readFile(path, 'utf-8'))
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === 'ENOENT') {
      return undefined
    }
    throw err
  }
}

// Written atomically (tmp file + rename) so a crash mid-write never leaves a corrupt
// checkpoint that a resumed run would fail to parse.
async function writeCheckpoint(path: string, afterUrl: string): Promise<void> {
  await mkdir(dirname(path), { recursive: true })
  const tmpPath = `${path}.${randomUUID()}.tmp`
  const checkpoint: Checkpoint = { afterUrl, updatedAt: new Date().toISOString() }
  await writeFile(tmpPath, JSON.stringify(checkpoint))
  await rename(tmpPath, path)
}

async function clearCheckpoint(path: string): Promise<void> {
  await rm(path, { force: true })
}

async function readCompletedRepoIds(path: string): Promise<Set<string>> {
  try {
    const ids = JSON.parse(await readFile(path, 'utf-8')) as string[]
    return new Set(ids)
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === 'ENOENT') {
      return new Set()
    }
    throw err
  }
}

// Written atomically (tmp file + rename), same as the checkpoint - a crash mid-write
// must never leave a corrupt file a resumed run would fail to parse.
async function writeCompletedRepoIds(path: string, ids: Set<string>): Promise<void> {
  await mkdir(dirname(path), { recursive: true })
  const tmpPath = `${path}.${randomUUID()}.tmp`
  await writeFile(tmpPath, JSON.stringify([...ids]))
  await rename(tmpPath, path)
}

async function clearCompletedRepoIds(path: string): Promise<void> {
  await rm(path, { force: true })
}

const main = async () => {
  const dryRun = process.argv.includes('--dry-run')
  const fresh = process.argv.includes('--fresh')
  const afterUrlOverride = readFlagValue('--after-url')
  const checkpointFile =
    process.env.STAR_SNAPSHOT_BACKFILL_CHECKPOINT_FILE ?? DEFAULT_CHECKPOINT_FILE
  const completedReposFile =
    process.env.STAR_SNAPSHOT_BACKFILL_COMPLETED_REPOS_FILE ?? DEFAULT_COMPLETED_REPOS_FILE
  const reservedCoreRateLimit = readIntEnv(
    'STAR_SNAPSHOT_BACKFILL_RESERVED_CORE_RATE_LIMIT',
    DEFAULT_RESERVED_CORE_RATE_LIMIT,
    true,
  )
  const concurrency = readIntEnv('STAR_SNAPSHOT_BACKFILL_CONCURRENCY', DEFAULT_CONCURRENCY, false)

  let afterUrl = afterUrlOverride
  if (!afterUrl && fresh) {
    await clearCheckpoint(checkpointFile)
  } else if (!afterUrl) {
    const checkpoint = await readCheckpoint(checkpointFile)
    if (checkpoint) {
      afterUrl = checkpoint.afterUrl
      log.info({ checkpoint }, 'resuming star snapshot backfill from checkpoint')
    }
  }

  // --fresh means ignore all prior progress, including which repos were already done -
  // otherwise stale entries would wrongly skip repos whose data was since cleared out.
  if (fresh) {
    await clearCompletedRepoIds(completedReposFile)
  }
  const completedRepoIds = fresh
    ? new Set<string>()
    : await readCompletedRepoIds(completedReposFile)

  log.info(
    {
      dryRun,
      fresh,
      afterUrl,
      checkpointFile,
      completedReposFile,
      alreadyBackfilledCount: completedRepoIds.size,
      reservedCoreRateLimit,
      concurrency,
    },
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
    completedRepoIds,
    isShuttingDown: () => shuttingDown,
    onProgress: async (progressAfterUrl) => {
      if (dryRun) return
      await Promise.all([
        writeCheckpoint(checkpointFile, progressAfterUrl),
        writeCompletedRepoIds(completedReposFile, completedRepoIds),
      ])
    },
  })

  if (totals.completed && !dryRun) {
    await clearCheckpoint(checkpointFile)
  }

  log.info({ ...totals, dryRun }, 'star snapshot backfill complete')
  process.exit(0)
}

main().catch((err) => {
  log.error({ err }, 'star snapshot backfill fatal error')
  process.exit(1)
})
