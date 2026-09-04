import { getPackagesDb, getPackagesDbConnection } from '../db'

export const DEFAULT_RESCORE_CHUNK_SIZE = 25000

export async function rescoreAllPackageRepos(
  chunkSize: number = DEFAULT_RESCORE_CHUNK_SIZE,
): Promise {
  const conn = await getPackagesDbConnection()
  const session = await conn.connect()
  try {
    const row = await session.one(`CALL rescore_package_repo_confidence(NULL::bigint[], $1, 0)`, [
      chunkSize,
    ])
    return row.applied_rows as number
  } finally {
    // The procedure holds a session-level advisory lock and COMMITs per chunk, so it cannot
    // release the lock from an exception handler — the caller has to, before the pool reuses
    // this connection.
    try {
      await session.none(`SELECT pg_advisory_unlock_all()`)
    } finally {
      session.done()
    }
  }
}

export async function countTiedPackageRepos(): Promise {
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
