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
         ingestion_source, last_synced_at, created_at, updated_at
       ) VALUES (
         $(purl), 'npm', $(namespace), $(name), $(status), $(registryUrl),
         $(description), $(homepage), $(declaredRepositoryUrl), $(repositoryUrl),
         $(licenses), $(licensesRaw), $(keywords),
         $(distLatest), $(distNext), $(distBeta),
         $(versionsCount), $(latestVersion), $(firstReleaseAt), $(latestReleaseAt),
         'npm-registry', NOW(), NOW(), NOW()
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
         versions_count          = EXCLUDED.versions_count,
         latest_version          = EXCLUDED.latest_version,
         first_release_at        = EXCLUDED.first_release_at,
         latest_release_at       = EXCLUDED.latest_release_at,
         ingestion_source        = EXCLUDED.ingestion_source,
         last_synced_at          = EXCLUDED.last_synced_at,
         updated_at              = EXCLUDED.updated_at
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
