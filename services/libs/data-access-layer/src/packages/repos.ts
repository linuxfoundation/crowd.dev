import { QueryExecutor } from '../queryExecutor'

import {
  KEEP_HIGHEST_CONFLICT_UPDATE,
  PackageRepoLinkClaim,
  claimFromRow,
  competingGithubRepoExpr,
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

// The writers below lock a package_repos row through the write itself, then take more
// locks in the ordered rescore; serializing per package first is what stops two writers
// on the same package from deadlocking.
async function lockPackage(qx: QueryExecutor, packageId: string): Promise<void> {
  await qx.result(
    `SELECT pg_advisory_xact_lock(hashtextextended('package_repos:' || $(packageId)::text, 0))`,
    { packageId },
  )
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
  await lockPackage(qx, packageId)

  const rowCount = await qx.result(
    `DELETE FROM package_repos
      WHERE package_id = $(packageId)::bigint
        AND source = 'declared'
        AND ($(exceptRepoId)::bigint IS NULL OR repo_id <> $(exceptRepoId)::bigint)`,
    { packageId, exceptRepoId: exceptRepoId ?? null },
  )
  if (rowCount === 0) return []

  await rescorePackageReposForPackages(qx, [packageId])
  return ['package_repos.repo_id']
}

// Conflict policy lives in KEEP_HIGHEST_CONFLICT_UPDATE: keep-highest across sources,
// replace on a same-source refresh.
export async function upsertPackageRepo(
  qx: QueryExecutor,
  packageId: string,
  repoId: string,
  claim: PackageRepoLinkClaim,
): Promise<string[]> {
  await lockPackage(qx, packageId)

  const row: { changed_fields: string[] } | null = await qx.selectOneOrNone(
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
         package_id, repo_id, source, provenance, confidence, created_at
       )
       SELECT $(packageId)::bigint, $(repoId)::bigint, $(source), $(provenance),
              scored.confidence, NOW()
         FROM scored
       ON CONFLICT (package_id, repo_id) DO UPDATE SET
         ${KEEP_HIGHEST_CONFLICT_UPDATE}
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
  if (!row) {
    throw new Error(`upsertPackageRepo: package ${packageId} or repo ${repoId} does not exist`)
  }

  await rescorePackageReposForPackages(qx, [packageId])
  return row.changed_fields
}

// The target CTE takes its row locks in primary-key order so a package-scoped and a
// repo-scoped rescore touching an overlapping set cannot deadlock against each other.
function rescoreQuery(targetPredicate: string): string {
  return `WITH target AS (
       SELECT pr.id
         FROM package_repos pr
        WHERE ${targetPredicate}
        ORDER BY pr.id
          FOR UPDATE
     )
     UPDATE package_repos pr
        SET confidence = s.confidence, verified_at = NOW()
       FROM target t
       JOIN package_repos cur ON cur.id = t.id
       JOIN packages p ON p.id = cur.package_id
       JOIN repos r ON r.id = cur.repo_id,
            LATERAL (
              SELECT ${packageRepoConfidenceCall('p', 'r', claimFromRow('cur'), competingGithubRepoExpr('cur.package_id', 'cur.repo_id'))} AS confidence
            ) s
      WHERE pr.id = t.id
        AND NOT (cur.source = 'deps_dev' AND cur.provenance IS NULL)
        AND s.confidence IS DISTINCT FROM cur.confidence`
}

export async function rescorePackageReposForPackages(
  qx: QueryExecutor,
  packageIds: string[],
): Promise<void> {
  if (packageIds.length === 0) return

  await qx.result(rescoreQuery(`pr.package_id = ANY($(packageIds)::bigint[])`), { packageIds })
}

// Rescores every link pointing at these repos. Called when the GitHub enricher flips
// archived / is_fork / disabled, since those are NULL at ingest time (the enricher runs
// after the registry writers) and carry penalties the original score could not apply.
export async function rescorePackageReposForRepos(
  qx: QueryExecutor,
  repoIds: string[],
): Promise<void> {
  if (repoIds.length === 0) return

  await qx.result(rescoreQuery(`pr.repo_id = ANY($(repoIds)::bigint[])`), { repoIds })
}

// A host change also flips the competing-GitHub penalty on the package's other links, which
// the repo-scoped set does not cover. Both sets are locked in one ordered pass: acquiring
// them in two nested calls lets a concurrent rescore hold a lower-id row of the second set
// while waiting on a row of the first.
export async function rescorePackageReposForRepoState(
  qx: QueryExecutor,
  repoIds: string[],
  hostChangedRepoIds: string[],
): Promise<void> {
  if (repoIds.length === 0) return

  await qx.result(
    rescoreQuery(`(pr.repo_id = ANY($(repoIds)::bigint[])
             OR pr.package_id IN (
                  SELECT hosted.package_id
                    FROM package_repos hosted
                   WHERE hosted.repo_id = ANY($(hostChangedRepoIds)::bigint[])
                ))`),
    { repoIds, hostChangedRepoIds },
  )
}
