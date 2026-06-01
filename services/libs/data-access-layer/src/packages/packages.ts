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
): Promise<{ id: string }> {
  return qx.selectOne(
    `INSERT INTO packages (
        purl, ecosystem, namespace, name, status, registry_url,
        description, homepage, declared_repository_url, repository_url,
        licenses, licenses_raw, keywords,
        dist_tags_latest, dist_tags_next, dist_tags_beta,
        versions_count, latest_version, first_release_at, latest_release_at,
        ingestion_source, last_synced_at
      ) VALUES (
        $(purl), 'npm', $(namespace), $(name), $(status), $(registryUrl),
        $(description), $(homepage), $(declaredRepositoryUrl), $(repositoryUrl),
        $(licenses), $(licensesRaw), $(keywords),
        $(distLatest), $(distNext), $(distBeta),
        $(versionsCount), $(latestVersion), $(firstReleaseAt), $(latestReleaseAt),
        'npm-registry', NOW()
      )
      ON CONFLICT (purl) DO UPDATE SET
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
        last_synced_at          = EXCLUDED.last_synced_at
      RETURNING id::text AS id`,
    input,
  )
}

export async function getTrackedNpmPackages(
  qx: QueryExecutor,
  names: string[],
): Promise<Array<{ id: string; name: string; purl: string; firstReleaseAt: string | null }>> {
  const rows: Array<{ id: string; name: string; purl: string; first_release_at: string | null }> =
    await qx.select(
      `SELECT id::text AS id, name, purl, first_release_at::text AS first_release_at
         FROM packages
        WHERE ecosystem = 'npm' AND name = ANY($(names)::text[])`,
      { names },
    )
  return rows.map((r) => ({
    id: r.id,
    name: r.name,
    purl: r.purl,
    firstReleaseAt: r.first_release_at,
  }))
}
