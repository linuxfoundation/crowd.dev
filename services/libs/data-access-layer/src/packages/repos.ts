import { QueryExecutor } from '../queryExecutor'

export async function getOrCreateRepoByUrl(
  qx: QueryExecutor,
  url: string,
  host: string,
): Promise<{ id: string; changedFields: string[] }> {
  // Repos are shared across packages (every package in a monorepo points at one repo)
  // so this is by far the common case
  const existing: { id: string } | null = await qx.selectOneOrNone(
    `SELECT id::text AS id FROM repos WHERE url = $(url)`,
    { url },
  )
  if (existing) return { id: existing.id, changedFields: [] }

  // Not seen yet — try to create it. ON CONFLICT DO NOTHING so a concurrent ingest lane creating
  // the same shared repo doesn't raise a unique violation.
  const inserted: { id: string } | null = await qx.selectOneOrNone(
    `INSERT INTO repos (url, host) VALUES ($(url), $(host))
     ON CONFLICT (url) DO NOTHING
     RETURNING id::text AS id`,
    { url, host },
  )
  if (inserted) return { id: inserted.id, changedFields: ['repos.url', 'repos.host'] }

  // Lost the race: another lane committed the same url between our SELECT and INSERT, so
  // ON CONFLICT DO NOTHING returned no row. Re-read in a fresh statement — under READ COMMITTED
  const row: { id: string } = await qx.selectOne(
    `SELECT id::text AS id FROM repos WHERE url = $(url)`,
    { url },
  )
  return { id: row.id, changedFields: [] }
}

export async function upsertPackageRepo(
  qx: QueryExecutor,
  packageId: string,
  repoId: string,
  source: string,
  confidence: number,
): Promise<string[]> {
  const row: { changed_fields: string[] } = await qx.selectOne(
    `WITH old AS (
       SELECT source, confidence FROM package_repos
        WHERE package_id = $(packageId)::bigint AND repo_id = $(repoId)::bigint
     ),
     ins AS (
       INSERT INTO package_repos (package_id, repo_id, source, confidence, created_at)
       VALUES ($(packageId)::bigint, $(repoId)::bigint, $(source), $(confidence), NOW())
       ON CONFLICT (package_id, repo_id) DO UPDATE SET
         source      = EXCLUDED.source,
         confidence  = EXCLUDED.confidence,
         verified_at = NOW()
       RETURNING source, confidence
     )
     SELECT array_remove(ARRAY[
       CASE WHEN o.source IS NULL                             THEN 'package_repos.repo_id' END,
       CASE WHEN o.source IS NULL
              OR o.source     IS DISTINCT FROM ins.source     THEN 'package_repos.source' END,
       CASE WHEN o.source IS NULL
              OR o.confidence IS DISTINCT FROM ins.confidence THEN 'package_repos.confidence' END
     ], NULL) AS changed_fields
     FROM ins LEFT JOIN old o ON true`,
    { packageId, repoId, source, confidence },
  )
  return row.changed_fields
}
