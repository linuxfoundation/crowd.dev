import { QueryExecutor } from '../queryExecutor'

export async function getOrCreateRepoByUrl(
  qx: QueryExecutor,
  url: string,
  host: string,
): Promise<{ id: string; changedFields: string[] }> {
  const row: { id: string; created: boolean } = await qx.selectOne(
    `
    WITH ins AS (
      INSERT INTO repos (url, host) VALUES ($(url), $(host))
      ON CONFLICT (url) DO NOTHING
      RETURNING id
    )
    SELECT id::text AS id, true AS created FROM ins
    UNION ALL
    SELECT id::text AS id, false AS created
      FROM repos
     WHERE url = $(url) AND NOT EXISTS (SELECT 1 FROM ins)
    `,
    { url, host },
  )
  return { id: row.id, changedFields: row.created ? ['repos.url', 'repos.host'] : [] }
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
       INSERT INTO package_repos (package_id, repo_id, source, confidence, created_at, updated_at)
       VALUES ($(packageId)::bigint, $(repoId)::bigint, $(source), $(confidence), NOW(), NOW())
       ON CONFLICT (package_id, repo_id) DO UPDATE SET
         source      = EXCLUDED.source,
         confidence  = EXCLUDED.confidence,
         verified_at = NOW(),
         updated_at  = EXCLUDED.updated_at
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
