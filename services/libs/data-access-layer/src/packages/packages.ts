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

export interface PackagistSeedRow {
  purl: string
  vendor: string
  name: string
}

export async function insertPackagistPackages(
  qx: QueryExecutor,
  rows: PackagistSeedRow[],
): Promise<number> {
  if (rows.length === 0) return 0
  const purls = rows.map((r) => r.purl)
  const vendors = rows.map((r) => r.vendor)
  const names = rows.map((r) => r.name)
  const result = await qx.result(
    `INSERT INTO packages (purl, ecosystem, namespace, name, registry_url, status, ingestion_source, last_synced_at, created_at)
     SELECT r.purl, 'packagist', r.vendor, r.name,
            'https://packagist.org/packages/' || r.vendor || '/' || r.name,
            'active', 'packagist-registry', NOW(), NOW()
       FROM unnest($(purls)::text[], $(vendors)::text[], $(names)::text[])
         AS r(purl, vendor, name)
      ON CONFLICT (purl) DO NOTHING`,
    { purls, vendors, names },
  )
  return result
}

export interface PackagistStatsUpdateInput {
  purl: string
  description: string | null
  declaredRepositoryUrl: string | null
  repositoryUrl: string | null
  status: string
  downloadsLast30d: number | null
  totalDownloads: number | null
  dependentCount: number | null
}

export async function updatePackagistPackageStats(
  qx: QueryExecutor,
  input: PackagistStatsUpdateInput,
): Promise<{ id: string; isCritical: boolean; changedFields: string[] } | null> {
  const row: { id: string; is_critical: boolean; changed_fields: string[] } | undefined =
    await qx.selectOneOrNone(
      `WITH old AS (
         SELECT description, declared_repository_url, repository_url, status,
                downloads_last_30d, total_downloads, dependent_count, ingestion_source
           FROM packages WHERE purl = $(purl) AND ecosystem = 'packagist'
       ),
       ins AS (
         UPDATE packages SET
           description               = $(description),
           declared_repository_url   = $(declaredRepositoryUrl),
           repository_url            = $(repositoryUrl),
           status                    = $(status),
           downloads_last_30d        = COALESCE($(downloadsLast30d), downloads_last_30d),
           total_downloads           = COALESCE($(totalDownloads), total_downloads),
           dependent_count           = COALESCE($(dependentCount), dependent_count),
           last_synced_at            = NOW()
         WHERE purl = $(purl) AND ecosystem = 'packagist'
         RETURNING id, is_critical, description, declared_repository_url, repository_url, status,
                   downloads_last_30d, total_downloads, dependent_count, ingestion_source
       )
       SELECT ins.id::text AS id, ins.is_critical,
              array_remove(ARRAY[
                CASE WHEN o.description               IS DISTINCT FROM ins.description               THEN 'packages.description' END,
                CASE WHEN o.declared_repository_url   IS DISTINCT FROM ins.declared_repository_url   THEN 'packages.declared_repository_url' END,
                CASE WHEN o.repository_url            IS DISTINCT FROM ins.repository_url            THEN 'packages.repository_url' END,
                CASE WHEN o.status                    IS DISTINCT FROM ins.status                    THEN 'packages.status' END,
                CASE WHEN o.downloads_last_30d        IS DISTINCT FROM ins.downloads_last_30d        THEN 'packages.downloads_last_30d' END,
                CASE WHEN o.total_downloads           IS DISTINCT FROM ins.total_downloads           THEN 'packages.total_downloads' END,
                CASE WHEN o.dependent_count           IS DISTINCT FROM ins.dependent_count           THEN 'packages.dependent_count' END,
                CASE WHEN o.ingestion_source          IS DISTINCT FROM ins.ingestion_source          THEN 'packages.ingestion_source' END
              ], NULL) AS changed_fields
         FROM ins LEFT JOIN old o ON true`,
      input,
    )

  if (!row) return null
  return { id: row.id, isCritical: row.is_critical, changedFields: row.changed_fields }
}

export interface PackagistVersionAggregates {
  versionsCount: number
  latestVersion: string | null
  firstReleaseAt: string | null
  latestReleaseAt: string | null
  licenses: string[] | null
  homepage: string | null
}

export async function updatePackagistVersionAggregates(
  qx: QueryExecutor,
  purl: string,
  agg: PackagistVersionAggregates,
): Promise<{ id: string; changedFields: string[] } | null> {
  const row: { id: string; changed_fields: string[] } | undefined = await qx.selectOneOrNone(
    `WITH old AS (
       SELECT versions_count, latest_version, first_release_at, latest_release_at, licenses, homepage, ingestion_source
         FROM packages WHERE purl = $(purl) AND ecosystem = 'packagist'
     ),
     ins AS (
       UPDATE packages SET
         versions_count     = CASE WHEN $(versionsCount) = 0 THEN packages.versions_count ELSE $(versionsCount) END,
         -- keep-on-zero like versions_count: a dev-only manifest must not null the
         -- known latest while the count is preserved
         latest_version     = CASE WHEN $(versionsCount) = 0 THEN packages.latest_version ELSE $(latestVersion) END,
         first_release_at   = COALESCE($(firstReleaseAt), first_release_at),
         latest_release_at  = COALESCE($(latestReleaseAt), latest_release_at),
         licenses           = COALESCE($(licenses), licenses),
         homepage           = COALESCE($(homepage), homepage),
         last_synced_at     = NOW()
       WHERE purl = $(purl) AND ecosystem = 'packagist'
       RETURNING id, versions_count, latest_version, first_release_at, latest_release_at, licenses, homepage, ingestion_source
     )
     SELECT ins.id::text AS id,
            array_remove(ARRAY[
              CASE WHEN o.versions_count     IS DISTINCT FROM ins.versions_count     THEN 'packages.versions_count' END,
              CASE WHEN o.latest_version     IS DISTINCT FROM ins.latest_version     THEN 'packages.latest_version' END,
              CASE WHEN o.first_release_at   IS DISTINCT FROM ins.first_release_at   THEN 'packages.first_release_at' END,
              CASE WHEN o.latest_release_at  IS DISTINCT FROM ins.latest_release_at  THEN 'packages.latest_release_at' END,
              CASE WHEN o.licenses           IS DISTINCT FROM ins.licenses           THEN 'packages.licenses' END,
              CASE WHEN o.homepage           IS DISTINCT FROM ins.homepage           THEN 'packages.homepage' END,
              CASE WHEN o.ingestion_source   IS DISTINCT FROM ins.ingestion_source   THEN 'packages.ingestion_source' END
            ], NULL) AS changed_fields
       FROM ins LEFT JOIN old o ON true`,
    { purl, ...agg },
  )

  if (!row) return null
  return { id: row.id, changedFields: row.changed_fields }
}

export async function getPackagistPackageIdsByNames(
  qx: QueryExecutor,
  names: string[],
): Promise<Map<string, string>> {
  // Composer names are case-insensitive but seed rows are stored lowercased —
  // lowercase the lookup so a manifest declaring 'Monolog/Monolog' still resolves.
  const lowerByOriginal = new Map(names.map((name) => [name, name.toLowerCase()]))
  const purls = [...lowerByOriginal.values()].map((name) => `pkg:composer/${name}`)

  const rows: Array<{ purl: string; id: string }> = await qx.select(
    `SELECT purl, id::text AS id FROM packages
      WHERE ecosystem = 'packagist' AND purl = ANY($(purls)::text[])`,
    { purls },
  )

  const idByLower = new Map<string, string>()
  for (const row of rows) {
    idByLower.set(row.purl.slice('pkg:composer/'.length), row.id)
  }

  // Map back keyed by the ORIGINAL (caller-supplied) names
  const map = new Map<string, string>()
  for (const [original, lower] of lowerByOriginal) {
    const id = idByLower.get(lower)
    if (id) map.set(original, id)
  }
  return map
}

export async function getCriticalPackagistPackageCount(qx: QueryExecutor): Promise<number> {
  const row: { count: string } = await qx.selectOne(
    `SELECT COUNT(*)::text AS count FROM packages WHERE ecosystem = 'packagist' AND is_critical = TRUE`,
  )
  return Number(row.count)
}

// How many critical PyPI packages exist — a cheap guard so the daily downloads workflow can
// skip its BigQuery scan entirely when there are none to ingest (the merge scopes to is_critical).
export async function getCriticalPypiPackageCount(qx: QueryExecutor): Promise<number> {
  const row: { count: string } = await qx.selectOne(
    `SELECT COUNT(*)::text AS count FROM packages WHERE ecosystem = 'pypi' AND is_critical = TRUE`,
  )
  return Number(row.count)
}
