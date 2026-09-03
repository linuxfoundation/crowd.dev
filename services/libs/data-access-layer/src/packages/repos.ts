import { QueryExecutor } from '../queryExecutor'

import {
  PackageRepoLinkClaim,
  claimFromRow,
  packageRepoConfidenceCall,
  packageRepoLinkClaimParams,
} from './repoConfidence'

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

// Removes a package's previous 'declared' repo link(s) when its manifest no longer
// resolves to a trusted repo (the field was removed, or no longer canonicalizes), or
// now resolves to a different one (pass exceptRepoId to keep only that fresh link —
// package_repos' unique key is (package_id, repo_id), not (package_id, source), so a
// plain upsert of the new link never removes a stale one pointing elsewhere). Only
// touches 'declared' — other sources (deps_dev, heuristic, manual) are owned by
// different pipelines and left alone.
export async function removeDeclaredPackageRepo(
  qx: QueryExecutor,
  packageId: string,
  exceptRepoId?: string,
): Promise<string[]> {
  const rowCount = await qx.result(
    `DELETE FROM package_repos
      WHERE package_id = $(packageId)::bigint
        AND source = 'declared'
        AND ($(exceptRepoId)::bigint IS NULL OR repo_id <> $(exceptRepoId)::bigint)`,
    { packageId, exceptRepoId: exceptRepoId ?? null },
  )
  return rowCount > 0 ? ['package_repos.repo_id'] : []
}

// Conflict policy: same-source refreshes always replace the stored claim so that updated
// ownership evidence (e.g. `no_evidence` → `unmatched`) is persisted. Cross-source,
// the highest-scoring claim wins — a stronger source (manual, an attested deps.dev row)
// cannot be downgraded by a weaker routine registry refresh.
export async function upsertPackageRepo(
  qx: QueryExecutor,
  packageId: string,
  repoId: string,
  claim: PackageRepoLinkClaim,
): Promise<string[]> {
  const row: { changed_fields: string[] } = await qx.selectOne(
    `WITH old AS (
       SELECT source, confidence FROM package_repos
        WHERE package_id = $(packageId)::bigint AND repo_id = $(repoId)::bigint
     ),
     scored AS (
       SELECT ${packageRepoConfidenceCall('p', 'r')} AS confidence
         FROM packages p, repos r
        WHERE p.id = $(packageId)::bigint AND r.id = $(repoId)::bigint
     ),
     ins AS (
       INSERT INTO package_repos (
         package_id, repo_id, source, signal, ownership_match, provenance,
         confidence, created_at
       )
       SELECT $(packageId)::bigint, $(repoId)::bigint, $(source), $(signal),
              $(ownershipMatch), $(provenance),
              scored.confidence, NOW()
         FROM scored
       ON CONFLICT (package_id, repo_id) DO UPDATE SET
         source           = CASE WHEN EXCLUDED.source = package_repos.source OR EXCLUDED.confidence > package_repos.confidence
                                 THEN EXCLUDED.source ELSE package_repos.source END,
         signal           = CASE WHEN EXCLUDED.source = package_repos.source OR EXCLUDED.confidence > package_repos.confidence
                                 THEN EXCLUDED.signal ELSE package_repos.signal END,
         ownership_match  = CASE WHEN EXCLUDED.source = package_repos.source OR EXCLUDED.confidence > package_repos.confidence
                                 THEN EXCLUDED.ownership_match ELSE package_repos.ownership_match END,
         provenance       = CASE WHEN EXCLUDED.source = package_repos.source OR EXCLUDED.confidence > package_repos.confidence
                                 THEN EXCLUDED.provenance ELSE package_repos.provenance END,
         confidence       = CASE WHEN EXCLUDED.source = package_repos.source
                                 THEN EXCLUDED.confidence
                                 ELSE GREATEST(EXCLUDED.confidence, package_repos.confidence) END,
         verified_at      = NOW()
       RETURNING source, confidence
     )
     SELECT array_remove(ARRAY[
       CASE WHEN o.source IS NULL                                         THEN 'package_repos.repo_id' END,
       CASE WHEN o.source IS NULL
              OR o.source           IS DISTINCT FROM ins.source           THEN 'package_repos.source' END,
       CASE WHEN o.source IS NULL
              OR o.confidence IS DISTINCT FROM ins.confidence THEN 'package_repos.confidence' END
     ], NULL) AS changed_fields
     FROM ins LEFT JOIN old o ON true`,
    { packageId, repoId, ...packageRepoLinkClaimParams(claim) },
  )
  await rescorePackageReposForPackages(qx, [packageId])
  return row.changed_fields
}

export async function rescorePackageReposForPackages(
  qx: QueryExecutor,
  packageIds: string[],
): Promise<void> {
  if (packageIds.length === 0) return

  await qx.result(
    `UPDATE package_repos pr
        SET confidence = s.confidence, verified_at = NOW()
       FROM packages p, repos r,
            LATERAL (
              SELECT ${packageRepoConfidenceCall('p', 'r', claimFromRow('pr'))} AS confidence
            ) s
      WHERE p.id = pr.package_id
        AND r.id = pr.repo_id
        AND pr.package_id = ANY($(packageIds)::bigint[])
        AND NOT (pr.source = 'deps_dev' AND pr.provenance IS NULL)
        AND s.confidence IS DISTINCT FROM pr.confidence`,
    { packageIds },
  )
}

// Rescores every link pointing at these repos. Called when the GitHub enricher flips
// archived / is_fork / disabled, since those are NULL at ingest time (the enricher runs
// after the registry writers) and carry penalties the original score could not apply.
export async function rescorePackageReposForRepos(
  qx: QueryExecutor,
  repoIds: string[],
): Promise<void> {
  if (repoIds.length === 0) return

  await qx.result(
    `UPDATE package_repos pr
        SET confidence = s.confidence, verified_at = NOW()
       FROM packages p, repos r,
            LATERAL (
              SELECT ${packageRepoConfidenceCall('p', 'r', claimFromRow('pr'))} AS confidence
            ) s
      WHERE p.id = pr.package_id
        AND r.id = pr.repo_id
        AND pr.repo_id = ANY($(repoIds)::bigint[])
        AND NOT (pr.source = 'deps_dev' AND pr.provenance IS NULL)
        AND s.confidence IS DISTINCT FROM pr.confidence`,
    { repoIds },
  )
}
