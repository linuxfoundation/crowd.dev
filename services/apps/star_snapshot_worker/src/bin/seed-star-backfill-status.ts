import { readFile } from 'fs/promises'

import { recordStarBackfillSuccess } from '@crowd/data-access-layer'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'

import { runWithConcurrency } from '../backfill/starSnapshotBackfill'

const log = getServiceLogger()

const DEFAULT_COMPLETED_REPOS_FILE = '/var/lib/star-snapshot-worker/backfill-completed-repos.json'
const SEED_CONCURRENCY = 10

async function readCompletedRepoIds(path: string): Promise<string[]> {
  return JSON.parse(await readFile(path, 'utf-8')) as string[]
}

// One-time seed: marks every repo the CM-1463 one-off backfill already completed as
// completedAt in repositoryStarBackfillStatus, using its checkpoint file as the source of
// truth. Without this, the self-heal schedule's first run has no way to tell those repos
// apart from ones that were never backfilled, and re-fetches full stargazer history for
// the entire estate. Safe to re-run - recordStarBackfillSuccess is an idempotent upsert.
const main = async () => {
  const completedReposFile =
    process.env.STAR_SNAPSHOT_BACKFILL_COMPLETED_REPOS_FILE ?? DEFAULT_COMPLETED_REPOS_FILE

  const repositoryIds = await readCompletedRepoIds(completedReposFile)
  log.info(
    { completedReposFile, count: repositoryIds.length },
    'seeding repositoryStarBackfillStatus from CM-1463 completed-repos checkpoint',
  )

  const conn = await getDbConnection(WRITE_DB_CONFIG())
  const qx = pgpQx(conn)
  await qx.selectOne('SELECT 1')
  log.info('Connected to database.')

  let seeded = 0
  let skipped = 0

  await runWithConcurrency(
    repositoryIds,
    SEED_CONCURRENCY,
    () => false,
    async (repositoryId) => {
      try {
        await recordStarBackfillSuccess(qx, repositoryId)
        seeded++
      } catch (err) {
        skipped++
        log.warn(
          { repositoryId, error: (err as Error)?.message ?? err },
          'failed to seed star backfill status for repo, skipping',
        )
      }
    },
  )

  log.info({ seeded, skipped, total: repositoryIds.length }, 'star backfill status seed complete')
  // A repo left unseeded here isn't lost - it just refetches its full history on the next
  // self-heal run - but a non-zero exit makes a partial run visible instead of silently green.
  process.exit(skipped > 0 ? 1 : 0)
}

main().catch((err) => {
  log.error({ err }, 'star backfill status seed fatal error')
  process.exit(1)
})
