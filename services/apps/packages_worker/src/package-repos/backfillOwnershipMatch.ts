import { getPackagesDbConnection } from '../db'

export const DEFAULT_OWNERSHIP_MATCH_BACKFILL_CHUNK_SIZE = 25000

// Mirrors rescore.ts's timeouts/session handling: a Temporal timeout (or Ctrl-C) can't
// cancel the in-flight CALL, so a chunk blocked on FOR UPDATE would hold the advisory
// lock forever. statement_timeout spans the whole CALL.
const BACKFILL_STATEMENT_TIMEOUT = '5h'
const BACKFILL_LOCK_TIMEOUT = '5min'

export async function backfillAllPackageRepoOwnershipMatch(
  chunkSize: number = DEFAULT_OWNERSHIP_MATCH_BACKFILL_CHUNK_SIZE,
): Promise<number> {
  const conn = await getPackagesDbConnection()
  const session = await conn.connect()
  try {
    await session.one(
      `SELECT set_config('statement_timeout', $1, false),
              set_config('lock_timeout', $2, false)`,
      [BACKFILL_STATEMENT_TIMEOUT, BACKFILL_LOCK_TIMEOUT],
    )
    const row = await session.one(`CALL backfill_package_repo_ownership_match($1, 0)`, [chunkSize])
    return row.applied_rows as number
  } finally {
    // One-off, long-running run — destroy the connection rather than hand back to the pool.
    session.done(true)
  }
}
