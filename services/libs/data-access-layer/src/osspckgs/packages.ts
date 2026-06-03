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
 * Carries everything the Maven enrichment path needs to sync a package.
 * For the Tier 2 (critical) path these fields come from `packages`; the disabled
 * non-critical path reads the same shape from `packages_universe`.
 */
export type MavenPackageToSync = Pick<
  IDbPackageUniverse,
  | 'id'
  | 'namespace'
  | 'name'
  | 'criticalityScore'
  | 'dependentPackagesCount'
  | 'dependentReposCount'
  | 'downloads30d'
> & {
  purl: string
  latestVersion: string | null
}

// ingestion_source values this worker writes once it has attempted a package
// (success or a terminal outcome). A `packages` row carrying any other value —
// e.g. the marker the criticality worker sets when it promotes a package to
// Tier 2 — has never been POM-enriched, so we pick it up immediately instead of
// waiting for the staleness window. Errors/skips re-run only once stale, so a
// broken package isn't retried every pass.
const MAVEN_WORKER_OUTCOMES = [
  'maven-registry',
  'maven_error',
  'maven_not_on_central',
  'maven_no_version',
]

/**
 * Returns a page of Maven packages that need syncing via POM extraction.
 *
 * isCritical=true  → Tier 2: reads from `packages` (populated by the criticality
 *                    worker, which writes ingestion_source + last_synced_at).
 *                    A row is due when it hasn't been POM-enriched yet, or is
 *                    stale by refreshDays. Ordered by criticality_score.
 * isCritical=false → disabled non-critical path: reads from `packages_universe`.
 *                    Kept for reference only — the universe→packages copy is owned
 *                    by the criticality worker and this path is not scheduled.
 */
export async function listMavenPackagesToSync(
  qx: QueryExecutor,
  options: { limit: number; refreshDays: number; isCritical: boolean },
): Promise<MavenPackageToSync[]> {
  const { limit, refreshDays, isCritical } = options

  if (isCritical) {
    return qx.select(
      `
      SELECT
        p.id,
        p.purl,
        p.namespace,
        p.name,
        p.criticality_score        AS "criticalityScore",
        p.dependent_packages_count AS "dependentPackagesCount",
        p.dependent_repos_count    AS "dependentReposCount",
        p.downloads_last_month     AS "downloads30d",
        p.latest_version           AS "latestVersion"
      FROM packages p
      WHERE
        p.ecosystem = 'maven'
        AND p.namespace IS NOT NULL
        AND (
          p.ingestion_source IS NULL
          OR p.ingestion_source <> ALL($(workerOutcomes)::text[])
          OR p.last_synced_at < NOW() - ($(refreshDays) || ' days')::interval
        )
      ORDER BY
        p.criticality_score DESC NULLS LAST,
        p.id ASC
      LIMIT $(limit)
      `,
      { limit, refreshDays, workerOutcomes: MAVEN_WORKER_OUTCOMES },
    )
  }

  // Disabled non-critical path — reads from packages_universe (not scheduled).
  return qx.select(
    `
    SELECT
      pu.id,
      pu.purl,
      pu.namespace,
      pu.name,
      pu.criticality_score        AS "criticalityScore",
      pu.dependent_packages_count AS "dependentPackagesCount",
      pu.dependent_repos_count    AS "dependentReposCount",
      pu.downloads_30d            AS "downloads30d",
      p.latest_version            AS "latestVersion"
    FROM packages_universe pu
    LEFT JOIN packages p ON p.purl = pu.purl
    WHERE
      pu.ecosystem = 'maven'
      AND pu.is_critical = false
      AND pu.purl IS NOT NULL
      AND pu.namespace IS NOT NULL
      AND (
        p.purl IS NULL
        OR p.last_synced_at < NOW() - ($(refreshDays) || ' days')::interval
      )
    ORDER BY
      pu.rank_in_ecosystem ASC NULLS LAST,
      pu.id ASC
    LIMIT $(limit)
    `,
    { limit, refreshDays },
  )
}

/**
 * Loads Tier 2 Maven packages (from `packages`) by package-level purl, regardless
 * of staleness. Used by the delta-API sync path: the upstream feed already told us
 * these packages changed, so we (re)extract them now. Purls not present in
 * `packages` (i.e. not promoted to Tier 2 by the criticality worker) are dropped.
 */
export async function listMavenPackagesByPurls(
  qx: QueryExecutor,
  purls: string[],
): Promise<MavenPackageToSync[]> {
  if (purls.length === 0) return []

  return qx.select(
    `
    SELECT
      p.id,
      p.purl,
      p.namespace,
      p.name,
      p.criticality_score        AS "criticalityScore",
      p.dependent_packages_count AS "dependentPackagesCount",
      p.dependent_repos_count    AS "dependentReposCount",
      p.downloads_last_month     AS "downloads30d",
      p.latest_version           AS "latestVersion"
    FROM packages p
    WHERE
      p.ecosystem = 'maven'
      AND p.namespace IS NOT NULL
      AND p.purl = ANY($(purls))
    ORDER BY
      p.criticality_score DESC NULLS LAST,
      p.id ASC
    `,
    { purls },
  )
}

// ─── packages touch ───────────────────────────────────────────────────────────

/**
 * Bumps last_synced_at without re-fetching POM data.
 * Used when the upstream version is unchanged — avoids a full extraction pass
 * while keeping the staleness timer fresh and syncing latest universe metrics.
 */
export async function touchPackageSyncedAt(
  qx: QueryExecutor,
  purl: string,
  metrics: {
    criticalityScore: number | null | undefined
    dependentPackagesCount: number | null | undefined
    dependentReposCount: number | null | undefined
    downloadsLastMonth: bigint | null | undefined
  },
): Promise<void> {
  await qx.result(
    `
    UPDATE packages SET
      last_synced_at           = NOW(),
      criticality_score        = COALESCE($(criticalityScore),       criticality_score),
      dependent_packages_count = COALESCE($(dependentPackagesCount), dependent_packages_count),
      dependent_repos_count    = COALESCE($(dependentReposCount),    dependent_repos_count),
      downloads_last_month     = COALESCE($(downloadsLastMonth),     downloads_last_month)
    WHERE purl = $(purl)
    `,
    {
      purl,
      criticalityScore: metrics.criticalityScore ?? null,
      dependentPackagesCount: metrics.dependentPackagesCount ?? null,
      dependentReposCount: metrics.dependentReposCount ?? null,
      downloadsLastMonth: metrics.downloadsLastMonth ?? null,
    },
  )
}

// ─── audit ────────────────────────────────────────────────────────────────────

export async function logAuditFieldChange(
  qx: QueryExecutor,
  worker: string,
  purl: string,
  changedFields: string[],
): Promise<void> {
  if (changedFields.length === 0) return
  await qx.result(
    `INSERT INTO audit_field_changes (worker, purl, changed_fields) VALUES ($(worker), $(purl), $(changedFields)::text[])`,
    { worker, purl, changedFields },
  )
}

// ─── packages upsert ──────────────────────────────────────────────────────────

/**
 * Inserts or updates a row in `packages`.
 * Returns the id and the list of fields that actually changed value.
 */
export async function upsertPackage(
  qx: QueryExecutor,
  item: IDbPackageUpsert,
): Promise<{ id: number; changedFields: string[] }> {
  const row = await qx.selectOne(
    `
    WITH old AS (
      SELECT description, homepage, registry_url, declared_repository_url, repository_url,
             licenses, licenses_raw, latest_version, ingestion_source
        FROM packages WHERE purl = $(purl)
    ),
    ins AS (
      INSERT INTO packages (
        purl, ecosystem, namespace, name,
        description, homepage, registry_url, declared_repository_url, repository_url,
        licenses, licenses_raw, latest_version,
        criticality_score, dependent_packages_count, dependent_repos_count, downloads_last_month,
        ingestion_source, last_synced_at
      ) VALUES (
        $(purl), $(ecosystem), $(namespace), $(name),
        $(description), $(homepage), $(registryUrl), $(declaredRepositoryUrl), $(repositoryUrl),
        $(licenses)::text[], $(licensesRaw), $(latestVersion),
        $(criticalityScore), $(dependentPackagesCount), $(dependentReposCount), $(downloadsLastMonth),
        $(ingestionSource), NOW()
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
      RETURNING id, description, homepage, registry_url, declared_repository_url, repository_url,
                licenses, licenses_raw, latest_version, ingestion_source
    )
    SELECT ins.id,
           array_remove(ARRAY[
             CASE WHEN o.description             IS DISTINCT FROM ins.description             THEN 'packages.description' END,
             CASE WHEN o.homepage                IS DISTINCT FROM ins.homepage                THEN 'packages.homepage' END,
             CASE WHEN o.registry_url            IS DISTINCT FROM ins.registry_url            THEN 'packages.registry_url' END,
             CASE WHEN o.declared_repository_url IS DISTINCT FROM ins.declared_repository_url THEN 'packages.declared_repository_url' END,
             CASE WHEN o.repository_url          IS DISTINCT FROM ins.repository_url          THEN 'packages.repository_url' END,
             CASE WHEN o.licenses                IS DISTINCT FROM ins.licenses                THEN 'packages.licenses' END,
             CASE WHEN o.licenses_raw            IS DISTINCT FROM ins.licenses_raw            THEN 'packages.licenses_raw' END,
             CASE WHEN o.latest_version          IS DISTINCT FROM ins.latest_version          THEN 'packages.latest_version' END,
             CASE WHEN o.ingestion_source        IS DISTINCT FROM ins.ingestion_source        THEN 'packages.ingestion_source' END
           ], NULL) AS changed_fields
    FROM ins LEFT JOIN old o ON true
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
  return { id: row.id as number, changedFields: row.changed_fields as string[] }
}
