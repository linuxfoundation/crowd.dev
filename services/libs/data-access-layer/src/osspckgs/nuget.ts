import { insertDailyDownloads } from '../packages/downloadsDaily'
import { upsertLast30dDownload } from '../packages/downloadsLast30d'
import { QueryExecutor } from '../queryExecutor'

// ─── Types ────────────────────────────────────────────────────────────────────

export type NuGetPackageToSync = {
  id: number
  purl: string
  name: string
  dependentPackagesCount: number | null
  dependentReposCount: number | null
  latestVersion: string | null
}

export type IDbNuGetPackageUpsert = {
  purl: string
  name: string
  description: string | null
  homepage: string | null
  declaredRepositoryUrl: string | null
  repositoryUrl: string | null
  licenses: string[] | null
  licensesRaw: string | null
  keywords: string[] | null
  status: string | null
  latestVersion: string | null
  versionsCount: number | null
  firstReleaseAt: Date | null
  latestReleaseAt: Date | null
  registryUrl: string | null
  ingestionSource: string
  dependentPackagesCount: number | null
  dependentReposCount: number | null
}

export type IDbNuGetVersionUpsert = {
  packageId: number
  name: string
  number: string
  publishedAt: Date | null
  isLatest: boolean
  isPrerelease: boolean
  isYanked: boolean | null
  licenses: string[] | null
  downloadCount: bigint | null
}

// ─── Package upsert ───────────────────────────────────────────────────────────

export async function upsertNuGetPackage(
  qx: QueryExecutor,
  item: IDbNuGetPackageUpsert,
): Promise<{ id: number; changedFields: string[] }> {
  const row = await qx.selectOne(
    `
    WITH old AS (
      SELECT description, homepage, declared_repository_url, repository_url,
             licenses, licenses_raw, keywords, status,
             latest_version, versions_count, first_release_at, latest_release_at,
             registry_url, ingestion_source
        FROM packages WHERE purl = $(purl)
    ),
    ins AS (
      INSERT INTO packages (
        purl, ecosystem, namespace, name,
        description, homepage, declared_repository_url, repository_url,
        licenses, licenses_raw, keywords, status,
        latest_version, versions_count, first_release_at, latest_release_at,
        registry_url, ingestion_source, dependent_count, dependent_repos_count,
        last_synced_at, created_at
      ) VALUES (
        $(purl), 'nuget', NULL, $(name),
        $(description), $(homepage), $(declaredRepositoryUrl), $(repositoryUrl),
        $(licenses)::text[], $(licensesRaw), $(keywords)::text[], $(status),
        $(latestVersion), $(versionsCount), $(firstReleaseAt), $(latestReleaseAt),
        $(registryUrl), $(ingestionSource), $(dependentPackagesCount), $(dependentReposCount),
        NOW(), NOW()
      )
      ON CONFLICT (purl) DO UPDATE SET
        description              = COALESCE(EXCLUDED.description,              packages.description),
        homepage                 = COALESCE(EXCLUDED.homepage,                 packages.homepage),
        declared_repository_url  = COALESCE(EXCLUDED.declared_repository_url,  packages.declared_repository_url),
        repository_url           = COALESCE(EXCLUDED.repository_url,           packages.repository_url),
        licenses                 = COALESCE(EXCLUDED.licenses,                 packages.licenses),
        licenses_raw             = COALESCE(EXCLUDED.licenses_raw,             packages.licenses_raw),
        keywords                 = COALESCE(EXCLUDED.keywords,                 packages.keywords),
        status                   = COALESCE(EXCLUDED.status,                   packages.status),
        latest_version           = COALESCE(EXCLUDED.latest_version,           packages.latest_version),
        versions_count           = COALESCE(EXCLUDED.versions_count,           packages.versions_count),
        first_release_at         = COALESCE(EXCLUDED.first_release_at,         packages.first_release_at),
        latest_release_at        = COALESCE(EXCLUDED.latest_release_at,        packages.latest_release_at),
        registry_url             = COALESCE(EXCLUDED.registry_url,             packages.registry_url),
        ingestion_source         = EXCLUDED.ingestion_source,
        dependent_count          = COALESCE(EXCLUDED.dependent_count,          packages.dependent_count),
        dependent_repos_count    = COALESCE(EXCLUDED.dependent_repos_count,    packages.dependent_repos_count),
        last_synced_at           = NOW()
      RETURNING id, description, homepage, declared_repository_url, repository_url,
                licenses, licenses_raw, keywords, status,
                latest_version, versions_count, first_release_at, latest_release_at,
                registry_url, ingestion_source
    )
    SELECT ins.id,
           array_remove(ARRAY[
             CASE WHEN o.description              IS DISTINCT FROM ins.description              THEN 'packages.description' END,
             CASE WHEN o.homepage                 IS DISTINCT FROM ins.homepage                 THEN 'packages.homepage' END,
             CASE WHEN o.declared_repository_url  IS DISTINCT FROM ins.declared_repository_url  THEN 'packages.declared_repository_url' END,
             CASE WHEN o.repository_url           IS DISTINCT FROM ins.repository_url           THEN 'packages.repository_url' END,
             CASE WHEN o.licenses                 IS DISTINCT FROM ins.licenses                 THEN 'packages.licenses' END,
             CASE WHEN o.licenses_raw             IS DISTINCT FROM ins.licenses_raw             THEN 'packages.licenses_raw' END,
             CASE WHEN o.keywords                 IS DISTINCT FROM ins.keywords                 THEN 'packages.keywords' END,
             CASE WHEN o.status                   IS DISTINCT FROM ins.status                   THEN 'packages.status' END,
             CASE WHEN o.latest_version           IS DISTINCT FROM ins.latest_version           THEN 'packages.latest_version' END,
             CASE WHEN o.versions_count           IS DISTINCT FROM ins.versions_count           THEN 'packages.versions_count' END,
             CASE WHEN o.first_release_at         IS DISTINCT FROM ins.first_release_at         THEN 'packages.first_release_at' END,
             CASE WHEN o.latest_release_at        IS DISTINCT FROM ins.latest_release_at        THEN 'packages.latest_release_at' END,
             CASE WHEN o.registry_url             IS DISTINCT FROM ins.registry_url             THEN 'packages.registry_url' END,
             CASE WHEN o.ingestion_source         IS DISTINCT FROM ins.ingestion_source         THEN 'packages.ingestion_source' END
           ], NULL) AS changed_fields
    FROM ins LEFT JOIN old o ON true
    `,
    {
      purl: item.purl,
      name: item.name,
      description: item.description ?? null,
      homepage: item.homepage ?? null,
      declaredRepositoryUrl: item.declaredRepositoryUrl ?? null,
      repositoryUrl: item.repositoryUrl ?? null,
      licenses: item.licenses ?? null,
      licensesRaw: item.licensesRaw ?? null,
      keywords: item.keywords ?? null,
      status: item.status ?? null,
      latestVersion: item.latestVersion ?? null,
      versionsCount: item.versionsCount ?? null,
      firstReleaseAt: item.firstReleaseAt ?? null,
      latestReleaseAt: item.latestReleaseAt ?? null,
      registryUrl: item.registryUrl ?? null,
      ingestionSource: item.ingestionSource,
      dependentPackagesCount: item.dependentPackagesCount ?? null,
      dependentReposCount: item.dependentReposCount ?? null,
    },
  )
  return { id: row.id as number, changedFields: row.changed_fields as string[] }
}

// ─── Versions bulk upsert ─────────────────────────────────────────────────────

export async function upsertNuGetVersionsBatch(
  qx: QueryExecutor,
  versions: IDbNuGetVersionUpsert[],
): Promise<string[]> {
  if (versions.length === 0) return []

  const packageId = versions[0].packageId
  if (versions.some((v) => v.packageId !== packageId)) {
    throw new Error('upsertNuGetVersionsBatch: all versions must belong to the same package')
  }

  const seen = new Set<string>()
  const deduped = versions.filter((v) => {
    if (seen.has(v.number)) return false
    seen.add(v.number)
    return true
  })

  const row: { changed_fields: string[] } = await qx.selectOne(
    `
    WITH old AS (
      SELECT number, is_latest, is_prerelease, is_yanked, licenses, download_count, published_at
        FROM versions
       WHERE package_id = $(packageId)::bigint AND number = ANY($(numbers)::text[])
    ),
    ins AS (
      INSERT INTO versions (package_id, ecosystem, namespace, name, number,
                            published_at, is_latest, is_prerelease, is_yanked,
                            licenses, download_count,
                            last_synced_at, created_at)
      SELECT
        $(packageId)::bigint, 'nuget', NULL, t.name, t.number,
        t.published_at::timestamptz, t.is_latest, t.is_prerelease,
        t.is_yanked,
        CASE WHEN t.license IS NULL THEN NULL ELSE ARRAY[t.license] END,
        t.download_count::bigint,
        NOW(), NOW()
      FROM UNNEST(
        $(names)::text[],
        $(numbers)::text[],
        $(publishedAts)::text[],
        $(isLatests)::bool[],
        $(isPreleases)::bool[],
        $(isYankeds)::bool[],
        $(licenses)::text[],
        $(downloadCounts)::bigint[]
      ) AS t(name, number, published_at, is_latest, is_prerelease, is_yanked, license, download_count)
      ON CONFLICT (package_id, number) DO UPDATE SET
        is_latest      = EXCLUDED.is_latest,
        is_prerelease  = EXCLUDED.is_prerelease,
        is_yanked      = COALESCE(EXCLUDED.is_yanked,      versions.is_yanked),
        licenses       = COALESCE(EXCLUDED.licenses,       versions.licenses),
        download_count = COALESCE(EXCLUDED.download_count, versions.download_count),
        published_at   = COALESCE(EXCLUDED.published_at,   versions.published_at),
        last_synced_at = NOW()
      RETURNING number, is_latest, is_prerelease, is_yanked, licenses, download_count, published_at
    )
    SELECT array_remove(ARRAY[
      CASE WHEN bool_or(o.number IS NULL)                                              THEN 'versions.number' END,
      CASE WHEN bool_or(o.is_latest      IS DISTINCT FROM ins.is_latest)               THEN 'versions.is_latest' END,
      CASE WHEN bool_or(o.is_prerelease  IS DISTINCT FROM ins.is_prerelease)           THEN 'versions.is_prerelease' END,
      CASE WHEN bool_or(o.is_yanked      IS DISTINCT FROM ins.is_yanked)               THEN 'versions.is_yanked' END,
      CASE WHEN bool_or(o.licenses       IS DISTINCT FROM ins.licenses)                THEN 'versions.licenses' END,
      CASE WHEN bool_or(o.download_count IS DISTINCT FROM ins.download_count)          THEN 'versions.download_count' END,
      CASE WHEN bool_or(o.published_at   IS DISTINCT FROM ins.published_at)            THEN 'versions.published_at' END
    ], NULL) AS changed_fields
    FROM ins LEFT JOIN old o ON o.number = ins.number
    `,
    {
      packageId,
      names: deduped.map((v) => v.name),
      numbers: deduped.map((v) => v.number),
      publishedAts: deduped.map((v) => (v.publishedAt ? v.publishedAt.toISOString() : null)),
      isLatests: deduped.map((v) => v.isLatest),
      isPreleases: deduped.map((v) => v.isPrerelease),
      isYankeds: deduped.map((v) => v.isYanked ?? null),
      licenses: deduped.map((v) => (v.licenses && v.licenses.length > 0 ? v.licenses[0] : null)),
      downloadCounts: deduped.map((v) =>
        v.downloadCount !== null ? Number(v.downloadCount) : null,
      ),
    },
  )
  return row.changed_fields
}

// ─── List packages to sync ────────────────────────────────────────────────────

// TODO: once critical packages are defined for NuGet, gate on is_critical when isCritical=true.
const NUGET_WORKER_OUTCOMES = ['nuget-registry', 'nuget_not_found', 'nuget_error']

export async function listNuGetPackagesToSync(
  qx: QueryExecutor,
  options: { limit: number; isCritical: boolean },
): Promise<NuGetPackageToSync[]> {
  const { limit, isCritical } = options
  return qx.select(
    `
    SELECT
      p.id,
      p.purl,
      p.name,
      p.dependent_count        AS "dependentPackagesCount",
      p.dependent_repos_count  AS "dependentReposCount",
      p.latest_version         AS "latestVersion"
    FROM packages p
    WHERE
      p.ecosystem = 'nuget'
      ${isCritical ? 'AND p.is_critical' : ''}
      AND (
        p.ingestion_source IS NULL
        OR p.ingestion_source <> ALL($(workerOutcomes)::text[])
        OR p.last_synced_at < NOW() - INTERVAL '1 day'
      )
    ORDER BY
      p.dependent_count DESC NULLS LAST,
      p.id ASC
    LIMIT $(limit)
    `,
    { limit, workerOutcomes: NUGET_WORKER_OUTCOMES },
  )
}

// ─── Download snapshot ────────────────────────────────────────────────────────

export async function recordNuGetDownloadSnapshot(
  qx: QueryExecutor,
  params: {
    packageId: number
    purl: string
    totalDownloads: number
    today: string // YYYY-MM-DD
  },
): Promise<string[]> {
  const { packageId, purl, totalDownloads, today } = params
  const changed: string[] = []

  const prevRow = await qx.selectOne(
    `SELECT total_downloads FROM packages WHERE id = $(packageId)`,
    { packageId },
  )
  const prev: number | null =
    prevRow?.total_downloads != null ? Number(prevRow.total_downloads) : null

  if (prev !== null && totalDownloads > prev) {
    const delta = totalDownloads - prev
    const dailyChanged = await insertDailyDownloads(qx, String(packageId), [
      { day: today, downloads: delta },
    ])
    dailyChanged.forEach((f) => changed.push(f))
  }

  const updated = await qx.result(
    `UPDATE packages SET total_downloads = $(totalDownloads)
      WHERE id = $(packageId)
        AND (total_downloads IS NULL OR total_downloads < $(totalDownloads))`,
    { totalDownloads, packageId },
  )
  if (updated > 0) changed.push('packages.total_downloads')

  const sumRow = await qx.selectOne(
    `SELECT COALESCE(SUM(count), 0)::bigint AS total
       FROM downloads_daily
      WHERE package_id = $(packageId)::bigint
        AND date > $(today)::date - INTERVAL '30 days'
        AND date <= $(today)::date`,
    { packageId, today },
  )
  const monthlySum = Number(sumRow.total)

  const thirtyDaysAgo = new Date(today)
  thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 29)
  const startDate = thirtyDaysAgo.toISOString().split('T')[0]

  const monthlyChanged = await upsertLast30dDownload(qx, purl, startDate, today, monthlySum, true)
  monthlyChanged.forEach((f) => changed.push(f))

  return changed
}
