import { QueryExecutor } from '../queryExecutor'
import { IDbPackageUniverse, IDbPackageUpsert } from './types'

export async function findPackageIdsByPurl(
  qx: QueryExecutor,
  purls: string[],
): Promise<Map<string, number>> {
  if (purls.length === 0) return new Map()
  const rows = await qx.select(`SELECT id, purl FROM packages WHERE purl = ANY($(purls))`, {
    purls,
  })
  return new Map(rows.map((r: { purl: string; id: number }) => [r.purl, r.id]))
}
// ─── packages_universe ────────────────────────────────────────────────────────

/**
 * Returns a page of Maven packages from packages_universe that need syncing
 * into the packages table.
 *
 * Eligibility rules:
 *  - p.purl IS NULL  → never added to packages (any criticality)
 *  - is_critical = false → periodic refresh of universe stats (default 180d)
 *  - is_critical = true  → not yet POM-enriched, new version released, or
 *                          periodic full refresh (default 90d)
 *
 * Critical packages are returned first so POM enrichment is prioritised.
 */
export async function listMavenPackagesToSync(
  qx: QueryExecutor,
  options: { limit: number; offset: number; fullRefreshDays?: number; nonCriticalRefreshDays?: number; isCritical?: boolean },
): Promise<
  (Pick<IDbPackageUniverse, 'id' | 'namespace' | 'name' | 'isCritical' | 'criticalityScore' | 'dependentPackagesCount' | 'dependentReposCount' | 'downloads30d'> & {
    purl: string
    latestVersion: string | null
  })[]
> {
  const { limit, offset, fullRefreshDays = 90, nonCriticalRefreshDays = 180, isCritical } = options
  const isCriticalFilter = isCritical !== undefined ? isCritical : null

  return qx.select(
    `
    SELECT
      pu.id,
      pu.purl,
      pu.namespace,
      pu.name,
      pu.is_critical              AS "isCritical",
      pu.criticality_score        AS "criticalityScore",
      pu.dependent_packages_count AS "dependentPackagesCount",
      pu.dependent_repos_count    AS "dependentReposCount",
      pu.downloads_30d            AS "downloads30d",
      p.latest_version            AS "latestVersion"
    FROM packages_universe pu
    LEFT JOIN packages p ON p.purl = pu.purl
    WHERE
      pu.ecosystem = 'maven'
      AND pu.purl IS NOT NULL
      AND pu.namespace IS NOT NULL
      AND ($(isCriticalFilter)::boolean IS NULL OR pu.is_critical = $(isCriticalFilter)::boolean)
      AND (
        p.purl IS NULL
        OR (pu.is_critical = false
            AND p.last_synced_at < NOW() - ($(nonCriticalRefreshDays) || ' days')::interval)
        OR (pu.is_critical = true
            AND p.ingestion_source IN ('maven_index', 'packages_universe'))
        OR (pu.is_critical = true
            AND p.latest_release_at > p.last_synced_at)
        OR (pu.is_critical = true
            AND p.last_synced_at < NOW() - ($(fullRefreshDays) || ' days')::interval)
      )
    ORDER BY
      pu.rank_in_ecosystem ASC NULLS LAST,
      pu.id ASC
    LIMIT $(limit) OFFSET $(offset)
    `,
    { limit, offset, fullRefreshDays, nonCriticalRefreshDays, isCriticalFilter },
  )
}

// ─── packages upsert ──────────────────────────────────────────────────────────

/**
 * Inserts or updates a row in `packages`.
 * Returns the id of the upserted row.
 */
export async function upsertPackage(qx: QueryExecutor, item: IDbPackageUpsert): Promise<number> {
  const row = await qx.selectOne(
    `
    INSERT INTO packages (
      purl,
      ecosystem,
      namespace,
      name,
      description,
      homepage,
      registry_url,
      declared_repository_url,
      repository_url,
      licenses,
      licenses_raw,
      latest_version,
      criticality_score,
      dependent_packages_count,
      dependent_repos_count,
      downloads_last_month,
      ingestion_source,
      last_synced_at
    ) VALUES (
      $(purl),
      $(ecosystem),
      $(namespace),
      $(name),
      $(description),
      $(homepage),
      $(registryUrl),
      $(declaredRepositoryUrl),
      $(repositoryUrl),
      $(licenses)::text[],
      $(licensesRaw),
      $(latestVersion),
      $(criticalityScore),
      $(dependentPackagesCount),
      $(dependentReposCount),
      $(downloadsLastMonth),
      $(ingestionSource),
      NOW()
    )
    ON CONFLICT (purl) DO UPDATE SET
      description              = COALESCE(EXCLUDED.description,              packages.description),
      homepage                 = COALESCE(EXCLUDED.homepage,                 packages.homepage),
      registry_url             = COALESCE(EXCLUDED.registry_url,             packages.registry_url),
      declared_repository_url  = COALESCE(EXCLUDED.declared_repository_url,  packages.declared_repository_url),
      repository_url           = COALESCE(EXCLUDED.repository_url,           packages.repository_url),
      licenses                 = COALESCE(EXCLUDED.licenses,                 packages.licenses),
      licenses_raw             = COALESCE(EXCLUDED.licenses_raw,             packages.licenses_raw),
      latest_version           = COALESCE(EXCLUDED.latest_version,           packages.latest_version),
      criticality_score        = COALESCE(EXCLUDED.criticality_score,        packages.criticality_score),
      dependent_packages_count = COALESCE(EXCLUDED.dependent_packages_count, packages.dependent_packages_count),
      dependent_repos_count    = COALESCE(EXCLUDED.dependent_repos_count,    packages.dependent_repos_count),
      downloads_last_month     = COALESCE(EXCLUDED.downloads_last_month,     packages.downloads_last_month),
      ingestion_source         = EXCLUDED.ingestion_source,
      last_synced_at           = NOW()
    RETURNING id
    `,
    {
      ...item,
      registryUrl: item.registryUrl ?? null,
      repositoryUrl: item.repositoryUrl ?? null,
      criticalityScore: item.criticalityScore ?? null,
      dependentPackagesCount: item.dependentPackagesCount ?? null,
      dependentReposCount: item.dependentReposCount ?? null,
      downloadsLastMonth: item.downloadsLastMonth ?? null,
    },
  )
  return row.id as number
}
