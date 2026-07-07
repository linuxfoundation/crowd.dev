import { QueryExecutor } from '../queryExecutor'

import { IDbPackageUniverse, IDbPackageUpsert, IDbSonatypePopularityUpsert } from './types'

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
  'id' | 'namespace' | 'name' | 'dependentPackagesCount' | 'dependentReposCount'
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
 *                    stale by refreshDays. Ordered by dependent_count.
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
        p.dependent_count          AS "dependentPackagesCount",
        p.dependent_repos_count    AS "dependentReposCount",
        p.latest_version           AS "latestVersion"
      FROM packages p
      WHERE
        p.ecosystem = 'maven'
        AND p.is_critical
        AND p.namespace IS NOT NULL
        AND (
          p.ingestion_source IS NULL
          OR p.ingestion_source <> ALL($(workerOutcomes)::text[])
          OR p.last_synced_at < NOW() - ($(refreshDays) || ' days')::interval
        )
      ORDER BY
        p.dependent_count DESC NULLS LAST,
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
      pu.dependent_count          AS "dependentPackagesCount",
      pu.dependent_repos_count    AS "dependentReposCount",
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

// ─── repository_url backfill ──────────────────────────────────────────────────

export type MavenRepoUrlRow = {
  id: number
  declaredRepositoryUrl: string | null
  repositoryUrl: string | null
}

/**
 * Keyset-paginated scan of Maven rows that carry a repository link (declared or
 * canonical). Rows with neither are skipped — there is nothing to recompute.
 * Used by the repository_url backfill to re-run the normalizer over stored data
 * without re-fetching POMs from the registry.
 *
 * `criticalOnly` restricts the scan to is_critical rows (index-backed by the
 * partial index on is_critical) — used for a fast, consumer-facing first pass.
 */
export async function listMavenPackagesForRepoUrlRecompute(
  qx: QueryExecutor,
  options: { afterId: number; limit: number; criticalOnly?: boolean },
): Promise<MavenRepoUrlRow[]> {
  return qx.select(
    `
    SELECT
      id,
      declared_repository_url AS "declaredRepositoryUrl",
      repository_url          AS "repositoryUrl"
    FROM packages
    WHERE ecosystem = 'maven'
      ${options.criticalOnly ? 'AND is_critical' : ''}
      AND id > $(afterId)
      AND (declared_repository_url IS NOT NULL OR repository_url IS NOT NULL)
    ORDER BY id ASC
    LIMIT $(limit)
    `,
    { afterId: options.afterId, limit: options.limit },
  )
}

/**
 * Applies a batch of recomputed repository_url values via direct UPDATE — the
 * only way to clear a stale value, since the enrichment upsert COALESCEs and
 * cannot write NULL. Splits clears (→ NULL) from sets to avoid NULLs inside a
 * text[] array literal.
 */
export async function updateMavenRepositoryUrls(
  qx: QueryExecutor,
  updates: { id: number; repositoryUrl: string | null }[],
): Promise<void> {
  if (updates.length === 0) return

  const toClear = updates.filter((u) => u.repositoryUrl === null).map((u) => u.id)
  const toSet = updates.filter(
    (u): u is { id: number; repositoryUrl: string } => u.repositoryUrl !== null,
  )

  if (toClear.length > 0) {
    await qx.result(
      `UPDATE packages SET repository_url = NULL
       WHERE id = ANY($(ids)::bigint[]) AND repository_url IS NOT NULL`,
      { ids: toClear },
    )
  }

  if (toSet.length > 0) {
    await qx.result(
      `
      UPDATE packages p
      SET repository_url = v.repository_url
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
    dependentPackagesCount: number | null | undefined
    dependentReposCount: number | null | undefined
  },
): Promise<void> {
  await qx.result(
    `
    UPDATE packages SET
      last_synced_at           = NOW(),
      dependent_count          = COALESCE($(dependentPackagesCount), dependent_count),
      dependent_repos_count    = COALESCE($(dependentReposCount),    dependent_repos_count)
    WHERE purl = $(purl)
    `,
    {
      purl,
      dependentPackagesCount: metrics.dependentPackagesCount ?? null,
      dependentReposCount: metrics.dependentReposCount ?? null,
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
             licenses, licenses_raw, latest_version, versions_count, latest_release_at, ingestion_source
        FROM packages WHERE purl = $(purl)
    ),
    ins AS (
      INSERT INTO packages (
        purl, ecosystem, namespace, name,
        description, homepage, registry_url, declared_repository_url, repository_url,
        licenses, licenses_raw, latest_version, versions_count, latest_release_at,
        dependent_count, dependent_repos_count,
        ingestion_source, last_synced_at, created_at
      ) VALUES (
        $(purl), $(ecosystem), $(namespace), $(name),
        $(description), $(homepage), $(registryUrl), $(declaredRepositoryUrl), $(repositoryUrl),
        $(licenses)::text[], $(licensesRaw), $(latestVersion), $(versionsCount), $(latestReleaseAt),
        $(dependentPackagesCount), $(dependentReposCount),
        $(ingestionSource), NOW(), NOW()
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
        versions_count           = COALESCE(EXCLUDED.versions_count,           packages.versions_count),
        latest_release_at        = COALESCE(EXCLUDED.latest_release_at,        packages.latest_release_at),
        dependent_count          = COALESCE(EXCLUDED.dependent_count,          packages.dependent_count),
        dependent_repos_count    = COALESCE(EXCLUDED.dependent_repos_count,    packages.dependent_repos_count),
        ingestion_source         = EXCLUDED.ingestion_source,
        last_synced_at           = NOW()
      RETURNING id, description, homepage, registry_url, declared_repository_url, repository_url,
                licenses, licenses_raw, latest_version, versions_count, latest_release_at, ingestion_source
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
             CASE WHEN o.versions_count          IS DISTINCT FROM ins.versions_count          THEN 'packages.versions_count' END,
             CASE WHEN o.latest_release_at       IS DISTINCT FROM ins.latest_release_at       THEN 'packages.latest_release_at' END,
             CASE WHEN o.ingestion_source        IS DISTINCT FROM ins.ingestion_source        THEN 'packages.ingestion_source' END
           ], NULL) AS changed_fields
    FROM ins LEFT JOIN old o ON true
    `,
    {
      ...item,
      registryUrl: item.registryUrl ?? null,
      repositoryUrl: item.repositoryUrl ?? null,
      versionsCount: item.versionsCount ?? null,
      latestReleaseAt: item.latestReleaseAt ?? null,
      dependentPackagesCount: item.dependentPackagesCount ?? null,
      dependentReposCount: item.dependentReposCount ?? null,
    },
  )
  return { id: row.id as number, changedFields: row.changed_fields as string[] }
}

// ─── sonatype popularity upsert ────────────────────────────────────────────────

/**
 * Upserts the Sonatype popularity signal for a Maven component, keyed by the
 * composite identity (ecosystem, namespace, name) — not purl — so it is immune
 * to purl format and matches the granularity of the unique index.
 *
 * Insert-if-missing: Sonatype's list is "what industry actually uses", so some
 * rows may not exist in `packages` yet. Those are inserted with
 * ingestion_source='sonatype' and only the identity + sonatype_* columns; the
 * rest is backfilled later by deps.dev / Maven enrichment.
 *
 * On conflict only the sonatype_* fields are updated — ingestion_source is
 * deliberately preserved so we never clobber how an existing row was ingested.
 *
 * Returns whether the row was inserted (true) or updated (false).
 */
export async function upsertSonatypePopularity(
  qx: QueryExecutor,
  item: IDbSonatypePopularityUpsert,
): Promise<{ id: number; inserted: boolean }> {
  const row = await qx.selectOne(
    `
    WITH existing AS (
      -- Evaluated against the pre-INSERT snapshot, so it reflects whether the row
      -- already existed. More robust than the xmax=0 trick for insert-vs-update.
      SELECT id FROM packages
       WHERE ecosystem = $(ecosystem)
         AND COALESCE(namespace, '') = COALESCE($(namespace), '')
         AND name = $(name)
    ),
    ins AS (
      INSERT INTO packages (
        purl, ecosystem, namespace, name,
        sonatype_popularity_score, sonatype_rank, sonatype_tier,
        sonatype_snapshot_at, sonatype_updated_at,
        ingestion_source
      ) VALUES (
        $(purl), $(ecosystem), $(namespace), $(name),
        $(sonatypePopularityScore), $(sonatypeRank), $(sonatypeTier),
        $(sonatypeSnapshotAt), NOW(),
        'sonatype'
      )
      ON CONFLICT (ecosystem, COALESCE(namespace, ''), name) DO UPDATE SET
        sonatype_popularity_score = EXCLUDED.sonatype_popularity_score,
        sonatype_rank             = EXCLUDED.sonatype_rank,
        sonatype_tier             = EXCLUDED.sonatype_tier,
        sonatype_snapshot_at      = EXCLUDED.sonatype_snapshot_at,
        sonatype_updated_at       = NOW()
        -- ingestion_source intentionally left untouched on update
      RETURNING id
    )
    SELECT ins.id, NOT EXISTS (SELECT 1 FROM existing) AS inserted
      FROM ins
    `,
    item,
  )
  return { id: row.id as number, inserted: row.inserted as boolean }
}
