import { QueryExecutor } from '../queryExecutor'

export async function getOrCreateRepoByUrl(
  qx: QueryExecutor,
  url: string,
): Promise<{ id: string; changedFields: string[] }> {
  const row: { id: string; created: boolean } = await qx.selectOne(
    `
    WITH ins AS (
      INSERT INTO repos (url) VALUES ($(url))
      ON CONFLICT (url) DO NOTHING
      RETURNING id
    )
    SELECT id::text AS id, true AS created FROM ins
    UNION ALL
    SELECT id::text AS id, false AS created
      FROM repos
     WHERE url = $(url) AND NOT EXISTS (SELECT 1 FROM ins)
    `,
    { url },
  )
  return { id: row.id, changedFields: row.created ? ['repos.url'] : [] }
}

export async function setPackageRepoForSource(
  qx: QueryExecutor,
  packageId: string,
  repoId: string,
  source: string,
  confidence: number,
): Promise<string[]> {
  const existing: { repo_id: string } | null = await qx.selectOneOrNone(
    `SELECT repo_id::text FROM package_repos
      WHERE package_id = $(packageId)::bigint AND source = $(source)`,
    { packageId, source },
  )

  if (!existing) {
    const rowCount = await qx.result(
      `INSERT INTO package_repos (package_id, repo_id, source, confidence)
       VALUES ($(packageId)::bigint, $(repoId)::bigint, $(source), $(confidence))
       ON CONFLICT (package_id, repo_id) DO NOTHING`,
      { packageId, repoId, source, confidence },
    )
    return rowCount > 0
      ? ['package_repos.repo_id', 'package_repos.source', 'package_repos.confidence']
      : []
  }

  if (existing.repo_id === repoId) return []

  await qx.result(
    `UPDATE package_repos
        SET repo_id = $(repoId)::bigint, confidence = $(confidence), verified_at = NOW()
      WHERE package_id = $(packageId)::bigint AND source = $(source)`,
    { packageId, repoId, source, confidence },
  )
  return ['package_repos.repo_id']
}
