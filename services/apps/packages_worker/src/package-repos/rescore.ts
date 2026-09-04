import { getPackagesDb, getPackagesDbConnection } from '../db'

export const DEFAULT_RESCORE_CHUNK_SIZE = 25000

// A Temporal timeout cannot cancel the in-flight CALL, so a chunk blocked on FOR UPDATE would hold
// the advisory lock forever, failing every later sweep. statement_timeout spans the whole CALL.
const RESCORE_STATEMENT_TIMEOUT = '5h'
const RESCORE_LOCK_TIMEOUT = '5min'

export async function rescoreAllPackageRepos(
  chunkSize: number = DEFAULT_RESCORE_CHUNK_SIZE,
): Promise<number> {
  const conn = await getPackagesDbConnection()
  const session = await conn.connect()
  try {
    await session.one(
      `SELECT set_config('statement_timeout', $1, false),
              set_config('lock_timeout', $2, false)`,
      [RESCORE_STATEMENT_TIMEOUT, RESCORE_LOCK_TIMEOUT],
    )
    const row = await session.one(`CALL rescore_package_repo_confidence(NULL::bigint[], $1, 0)`, [
      chunkSize,
    ])
    return row.applied_rows as number
  } finally {
    // The procedure holds a session-level advisory lock and COMMITs per chunk, so it cannot
    // release the lock from an exception handler — the caller has to, before the pool reuses
    // this connection.
    let unlocked = false
    try {
      await session.one(`SELECT pg_advisory_unlock_all()`)
      unlocked = true
    } finally {
      session.done(!unlocked)
    }
  }
}

export async function countTiedPackageRepos(): Promise<number> {
  const qx = await getPackagesDb()
  const row = await qx.selectOne(`
    SELECT COUNT(*)::int AS tied_packages
    FROM (
      SELECT package_id
      FROM package_repos
      GROUP BY package_id
      HAVING COUNT(*) <> COUNT(DISTINCT confidence)
    ) t
  `)
  return row.tied_packages as number
}
