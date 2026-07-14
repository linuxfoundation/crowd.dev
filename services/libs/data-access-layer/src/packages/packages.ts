import { QueryExecutor } from '../queryExecutor'

export interface NpmPackageUpsertInput {
  purl: string
  namespace: string | null
  name: string
  status: string
  registryUrl: string
  description: string | null
  homepage: string | null
  declaredRepositoryUrl: string | null
  repositoryUrl: string | null
  licenses: string[] | null
  licensesRaw: string | null
  keywords: string[] | null
  distLatest: string | null
  distNext: string | null
  distBeta: string | null
  versionsCount: number
  latestVersion: string | null
  firstReleaseAt: string | null
  latestReleaseAt: string | null
}

export async function upsertNpmPackage(
  qx: QueryExecutor,
  input: NpmPackageUpsertInput,
): Promise<{ id: string; changedFields: string[] }> {
  const row: { id: string; changed_fields: string[] } = await qx.selectOne(
    `WITH old AS (
       SELECT namespace, name, status, registry_url, description, homepage,
              declared_repository_url, repository_url, licenses, licenses_raw, keywords,
              dist_tags_latest, dist_tags_next, dist_tags_beta,
              versions_count, latest_version, first_release_at, latest_release_at,
              ingestion_source
         FROM packages WHERE purl = $(purl)
     ),
     ins AS (
       INSERT INTO packages (
         purl, ecosystem, namespace, name, status, registry_url,
         description, homepage, declared_repository_url, repository_url,
         licenses, licenses_raw, keywords,
         dist_tags_latest, dist_tags_next, dist_tags_beta,
         versions_count, latest_version, first_release_at, latest_release_at,
         ingestion_source, last_synced_at, created_at
       ) VALUES (
         $(purl), 'npm', $(namespace), $(name), $(status), $(registryUrl),
         $(description), $(homepage), $(declaredRepositoryUrl), $(repositoryUrl),
         $(licenses), $(licensesRaw), $(keywords),
         $(distLatest), $(distNext), $(distBeta),
         $(versionsCount), $(latestVersion), $(firstReleaseAt), $(latestReleaseAt),
         'npm-registry', NOW(), NOW()
       )
       ON CONFLICT (purl) DO UPDATE SET
         namespace               = EXCLUDED.namespace,
         name                    = EXCLUDED.name,
         status                  = EXCLUDED.status,
         registry_url            = EXCLUDED.registry_url,
         description             = EXCLUDED.description,
         homepage                = EXCLUDED.homepage,
         declared_repository_url = EXCLUDED.declared_repository_url,
         repository_url          = EXCLUDED.repository_url,
         licenses                = EXCLUDED.licenses,
         licenses_raw            = EXCLUDED.licenses_raw,
         keywords                = EXCLUDED.keywords,
         dist_tags_latest        = EXCLUDED.dist_tags_latest,
         dist_tags_next          = EXCLUDED.dist_tags_next,
         dist_tags_beta          = EXCLUDED.dist_tags_beta,
         -- An unpublished stub reports 0 versions; don't clobber a previously-known count
         -- (keep it consistent with the version rows that are retained on unpublish).
         versions_count          = CASE WHEN EXCLUDED.versions_count = 0
                                        THEN packages.versions_count
                                        ELSE EXCLUDED.versions_count END,
         latest_version          = EXCLUDED.latest_version,
         first_release_at        = COALESCE(EXCLUDED.first_release_at, packages.first_release_at),
         latest_release_at       = COALESCE(EXCLUDED.latest_release_at, packages.latest_release_at),
         ingestion_source        = EXCLUDED.ingestion_source,
         last_synced_at          = EXCLUDED.last_synced_at
       RETURNING id, namespace, name, status, registry_url, description, homepage,
                 declared_repository_url, repository_url, licenses, licenses_raw, keywords,
                 dist_tags_latest, dist_tags_next, dist_tags_beta,
                 versions_count, latest_version, first_release_at, latest_release_at,
                 ingestion_source
     )
     SELECT ins.id::text AS id,
            array_remove(ARRAY[
              CASE WHEN o.namespace               IS DISTINCT FROM ins.namespace               THEN 'packages.namespace' END,
              CASE WHEN o.name                    IS DISTINCT FROM ins.name                    THEN 'packages.name' END,
              CASE WHEN o.status                  IS DISTINCT FROM ins.status                  THEN 'packages.status' END,
              CASE WHEN o.registry_url            IS DISTINCT FROM ins.registry_url            THEN 'packages.registry_url' END,
              CASE WHEN o.description             IS DISTINCT FROM ins.description             THEN 'packages.description' END,
              CASE WHEN o.homepage                IS DISTINCT FROM ins.homepage                THEN 'packages.homepage' END,
              CASE WHEN o.declared_repository_url IS DISTINCT FROM ins.declared_repository_url THEN 'packages.declared_repository_url' END,
              CASE WHEN o.repository_url          IS DISTINCT FROM ins.repository_url          THEN 'packages.repository_url' END,
              CASE WHEN o.licenses                IS DISTINCT FROM ins.licenses                THEN 'packages.licenses' END,
              CASE WHEN o.licenses_raw            IS DISTINCT FROM ins.licenses_raw            THEN 'packages.licenses_raw' END,
              CASE WHEN o.keywords                IS DISTINCT FROM ins.keywords                THEN 'packages.keywords' END,
              CASE WHEN o.dist_tags_latest        IS DISTINCT FROM ins.dist_tags_latest        THEN 'packages.dist_tags_latest' END,
              CASE WHEN o.dist_tags_next          IS DISTINCT FROM ins.dist_tags_next          THEN 'packages.dist_tags_next' END,
              CASE WHEN o.dist_tags_beta          IS DISTINCT FROM ins.dist_tags_beta          THEN 'packages.dist_tags_beta' END,
              CASE WHEN o.versions_count          IS DISTINCT FROM ins.versions_count          THEN 'packages.versions_count' END,
              CASE WHEN o.latest_version          IS DISTINCT FROM ins.latest_version          THEN 'packages.latest_version' END,
              CASE WHEN o.first_release_at        IS DISTINCT FROM ins.first_release_at        THEN 'packages.first_release_at' END,
              CASE WHEN o.latest_release_at       IS DISTINCT FROM ins.latest_release_at       THEN 'packages.latest_release_at' END,
              CASE WHEN o.ingestion_source        IS DISTINCT FROM ins.ingestion_source        THEN 'packages.ingestion_source' END
            ], NULL) AS changed_fields
       FROM ins LEFT JOIN old o ON true`,
    input,
  )
  return { id: row.id, changedFields: row.changed_fields }
}

export interface PypiPackageUpsertInput {
  purl: string
  namespace: string | null
  name: string
  status: string
  registryUrl: string
  description: string | null
  homepage: string | null
  declaredRepositoryUrl: string | null
  repositoryUrl: string | null
  licenses: string[] | null
  licensesRaw: string | null
  keywords: string[] | null
  versionsCount: number
  latestVersion: string | null
  firstReleaseAt: string | null
  latestReleaseAt: string | null
}

export async function upsertPypiPackage(
  qx: QueryExecutor,
  input: PypiPackageUpsertInput,
): Promise<{ id: string; changedFields: string[] }> {
  const row: { id: string; changed_fields: string[] } = await qx.selectOne(
    `WITH old AS (
       SELECT namespace, name, status, registry_url, description, homepage,
              declared_repository_url, repository_url, licenses, licenses_raw, keywords,
              versions_count, latest_version, first_release_at, latest_release_at,
              ingestion_source
         FROM packages WHERE purl = $(purl)
     ),
     ins AS (
       INSERT INTO packages (
         purl, ecosystem, namespace, name, status, registry_url,
         description, homepage, declared_repository_url, repository_url,
         licenses, licenses_raw, keywords,
         versions_count, latest_version, first_release_at, latest_release_at,
         ingestion_source, last_synced_at, created_at
       ) VALUES (
         $(purl), 'pypi', $(namespace), $(name), $(status), $(registryUrl),
         $(description), $(homepage), $(declaredRepositoryUrl), $(repositoryUrl),
         $(licenses), $(licensesRaw), $(keywords),
         $(versionsCount), $(latestVersion), $(firstReleaseAt), $(latestReleaseAt),
         'pypi-registry', NOW(), NOW()
       )
       ON CONFLICT (purl) DO UPDATE SET
         namespace               = EXCLUDED.namespace,
         name                    = EXCLUDED.name,
         status                  = EXCLUDED.status,
         registry_url            = EXCLUDED.registry_url,
         description             = EXCLUDED.description,
         homepage                = EXCLUDED.homepage,
         declared_repository_url = EXCLUDED.declared_repository_url,
         repository_url          = EXCLUDED.repository_url,
         licenses                = EXCLUDED.licenses,
         licenses_raw            = EXCLUDED.licenses_raw,
         keywords                = EXCLUDED.keywords,
         -- A package with no released files reports 0 versions; don't clobber a
         -- previously-known count (matches the retained version rows).
         versions_count          = CASE WHEN EXCLUDED.versions_count = 0
                                        THEN packages.versions_count
                                        ELSE EXCLUDED.versions_count END,
         latest_version          = EXCLUDED.latest_version,
         first_release_at        = COALESCE(EXCLUDED.first_release_at, packages.first_release_at),
         latest_release_at       = COALESCE(EXCLUDED.latest_release_at, packages.latest_release_at),
         ingestion_source        = EXCLUDED.ingestion_source,
         last_synced_at          = EXCLUDED.last_synced_at
       RETURNING id, namespace, name, status, registry_url, description, homepage,
                 declared_repository_url, repository_url, licenses, licenses_raw, keywords,
                 versions_count, latest_version, first_release_at, latest_release_at,
                 ingestion_source
     )
     SELECT ins.id::text AS id,
            array_remove(ARRAY[
              CASE WHEN o.namespace               IS DISTINCT FROM ins.namespace               THEN 'packages.namespace' END,
              CASE WHEN o.name                    IS DISTINCT FROM ins.name                    THEN 'packages.name' END,
              CASE WHEN o.status                  IS DISTINCT FROM ins.status                  THEN 'packages.status' END,
              CASE WHEN o.registry_url            IS DISTINCT FROM ins.registry_url            THEN 'packages.registry_url' END,
              CASE WHEN o.description             IS DISTINCT FROM ins.description             THEN 'packages.description' END,
              CASE WHEN o.homepage                IS DISTINCT FROM ins.homepage                THEN 'packages.homepage' END,
              CASE WHEN o.declared_repository_url IS DISTINCT FROM ins.declared_repository_url THEN 'packages.declared_repository_url' END,
              CASE WHEN o.repository_url          IS DISTINCT FROM ins.repository_url          THEN 'packages.repository_url' END,
              CASE WHEN o.licenses                IS DISTINCT FROM ins.licenses                THEN 'packages.licenses' END,
              CASE WHEN o.licenses_raw            IS DISTINCT FROM ins.licenses_raw            THEN 'packages.licenses_raw' END,
              CASE WHEN o.keywords                IS DISTINCT FROM ins.keywords                THEN 'packages.keywords' END,
              CASE WHEN o.versions_count          IS DISTINCT FROM ins.versions_count          THEN 'packages.versions_count' END,
              CASE WHEN o.latest_version          IS DISTINCT FROM ins.latest_version          THEN 'packages.latest_version' END,
              CASE WHEN o.first_release_at        IS DISTINCT FROM ins.first_release_at        THEN 'packages.first_release_at' END,
              CASE WHEN o.latest_release_at       IS DISTINCT FROM ins.latest_release_at       THEN 'packages.latest_release_at' END,
              CASE WHEN o.ingestion_source        IS DISTINCT FROM ins.ingestion_source        THEN 'packages.ingestion_source' END
            ], NULL) AS changed_fields
       FROM ins LEFT JOIN old o ON true`,
    input,
  )
  return { id: row.id, changedFields: row.changed_fields }
}

export async function getTrackedNpmPackages(
  qx: QueryExecutor,
  purls: string[],
): Promise<Array<{ id: string; name: string; purl: string; firstReleaseAt: string | null }>> {
  const rows: Array<{ id: string; name: string; purl: string; first_release_at: string | null }> =
    await qx.select(
      `SELECT id::text AS id, name, purl, first_release_at::text AS first_release_at
         FROM packages
        WHERE ecosystem = 'npm' AND purl = ANY($(purls)::text[])`,
      { purls },
    )
  return rows.map((r) => ({
    id: r.id,
    name: r.name,
    purl: r.purl,
    firstReleaseAt: r.first_release_at,
  }))
}

// ─── npm repository_url backfill ──────────────────────────────────────────────

export type NpmRepoUrlRow = {
  id: string
  declaredRepositoryUrl: string | null
  repositoryUrl: string | null
}

/**
 * Keyset-paginated scan of npm rows that carry a declared repository value.
 * The backfill recomputes repository_url *from* declared_repository_url, so rows
 * with no declared value are skipped — there is nothing to recompute from, and a
 * null declaration must never be used to clear an existing repository_url that a
 * different source may have set.
 *
 * `criticalOnly` restricts the scan to is_critical rows — used for a fast,
 * consumer-facing first pass.
 */
export async function listNpmPackagesForRepoUrlRecompute(
  qx: QueryExecutor,
  options: { afterId: string; limit: number; criticalOnly?: boolean },
): Promise<NpmRepoUrlRow[]> {
  const rows: Array<{
    id: string
    declared_repository_url: string | null
    repository_url: string | null
  }> = await qx.select(
    `
      SELECT
        packages.id::text AS id,
        declared_repository_url,
        repository_url
      FROM packages
      WHERE ecosystem = 'npm'
        ${options.criticalOnly ? 'AND is_critical' : ''}
        AND id > $(afterId)::bigint
        AND declared_repository_url IS NOT NULL
      ORDER BY packages.id ASC
      LIMIT $(limit)
      `,
    { afterId: options.afterId, limit: options.limit },
  )
  return rows.map((r) => ({
    id: r.id,
    declaredRepositoryUrl: r.declared_repository_url,
    repositoryUrl: r.repository_url,
  }))
}

/**
 * Applies a batch of recomputed repository_url values via direct UPDATE — the
 * only way to clear a stale value, since the enrichment upsert writes
 * EXCLUDED.repository_url unconditionally but is only ever called with a fresh
 * packument fetch. Splits clears (→ NULL) from sets to avoid NULLs inside a
 * text[] array literal. Also bumps last_synced_at so the correction is picked
 * up by any downstream export keyed off it.
 */
export async function updateNpmRepositoryUrls(
  qx: QueryExecutor,
  updates: { id: string; repositoryUrl: string | null }[],
): Promise<void> {
  if (updates.length === 0) return

  const toClear = updates.filter((u) => u.repositoryUrl === null).map((u) => u.id)
  const toSet = updates.filter(
    (u): u is { id: string; repositoryUrl: string } => u.repositoryUrl !== null,
  )

  if (toClear.length > 0) {
    await qx.result(
      `UPDATE packages SET repository_url = NULL, last_synced_at = NOW()
       WHERE id = ANY($(ids)::bigint[]) AND repository_url IS NOT NULL`,
      { ids: toClear },
    )
  }

  if (toSet.length > 0) {
    await qx.result(
      `
      UPDATE packages p
      SET repository_url = v.repository_url, last_synced_at = NOW()
      FROM (
        SELECT unnest($(ids)::bigint[]) AS id,
               unnest($(urls)::text[])  AS repository_url
      ) v
      WHERE p.id = v.id
        AND p.repository_url IS DISTINCT FROM v.repository_url
      `,
      { ids: toSet.map((u) => u.id), urls: toSet.map((u) => u.repositoryUrl) },
    )
  }
}

// How many critical PyPI packages exist — a cheap guard so the daily downloads workflow can
// skip its BigQuery scan entirely when there are none to ingest (the merge scopes to is_critical).
export async function getCriticalPypiPackageCount(qx: QueryExecutor): Promise<number> {
  const row: { count: string } = await qx.selectOne(
    `SELECT COUNT(*)::text AS count FROM packages WHERE ecosystem = 'pypi' AND is_critical = TRUE`,
  )
  return Number(row.count)
}
