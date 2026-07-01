import { QueryExecutor } from '../queryExecutor'

export interface NpmVersionInput {
  number: string
  publishedAt: string | null
  isLatest: boolean
  isPrerelease: boolean
  // Single SPDX string from the packument; stored as a one-element text[].
  license: string | null
}

export async function upsertNpmVersions(
  qx: QueryExecutor,
  packageId: string,
  versions: NpmVersionInput[],
): Promise<string[]> {
  if (versions.length === 0) return []
  const row: { changed_fields: string[] } = await qx.selectOne(
    `WITH old AS (
       SELECT number, published_at, is_latest, is_prerelease, licenses
         FROM versions
        WHERE package_id = $(packageId)::bigint AND number = ANY($(numbers)::text[])
     ),
     ins AS (
       INSERT INTO versions (
         package_id, ecosystem, namespace, name, number,
         published_at, is_latest, is_prerelease, licenses, last_synced_at,
         created_at
       )
       SELECT $(packageId)::bigint, 'npm', p.namespace, p.name, v.num,
              v.pub::timestamptz, v.latest, v.pre,
              CASE WHEN v.lic IS NULL THEN NULL::text[] ELSE ARRAY[v.lic] END,
              NOW(), NOW()
       FROM unnest(
         $(numbers)::text[],
         $(publishedAts)::text[],
         $(isLatests)::bool[],
         $(isPrereleases)::bool[],
         $(licenses)::text[]
       ) AS v(num, pub, latest, pre, lic)
       CROSS JOIN (SELECT namespace, name FROM packages WHERE id = $(packageId)::bigint) p
       ON CONFLICT (package_id, number) DO UPDATE SET
         -- COALESCE so a NULL upstream value never wipes a known date, but a
         -- non-null correction (or first-known timestamp) is applied.
         published_at   = COALESCE(EXCLUDED.published_at, versions.published_at),
         is_latest      = EXCLUDED.is_latest,
         is_prerelease  = EXCLUDED.is_prerelease,
         licenses       = EXCLUDED.licenses,
         last_synced_at = EXCLUDED.last_synced_at
       RETURNING number, published_at, is_latest, is_prerelease, licenses
     )
     SELECT array_remove(ARRAY[
       CASE WHEN bool_or(o.number IS NULL)                                THEN 'versions.number' END,
       CASE WHEN bool_or(o.number IS NULL OR o.published_at IS DISTINCT FROM ins.published_at) THEN 'versions.published_at' END,
       CASE WHEN bool_or(o.is_latest     IS DISTINCT FROM ins.is_latest)     THEN 'versions.is_latest' END,
       CASE WHEN bool_or(o.is_prerelease IS DISTINCT FROM ins.is_prerelease) THEN 'versions.is_prerelease' END,
       CASE WHEN bool_or(o.licenses      IS DISTINCT FROM ins.licenses)      THEN 'versions.licenses' END
     ], NULL) AS changed_fields
     FROM ins LEFT JOIN old o ON o.number = ins.number`,
    {
      packageId,
      numbers: versions.map((v) => v.number),
      publishedAts: versions.map((v) => v.publishedAt),
      isLatests: versions.map((v) => v.isLatest),
      isPrereleases: versions.map((v) => v.isPrerelease),
      licenses: versions.map((v) => v.license),
    },
  )
  return row.changed_fields
}

export interface PypiVersionInput {
  number: string
  publishedAt: string | null
  isLatest: boolean
  isPrerelease: boolean
  isYanked: boolean
  license: string | null
}

export async function upsertPypiVersions(
  qx: QueryExecutor,
  packageId: string,
  versions: PypiVersionInput[],
): Promise<string[]> {
  if (versions.length === 0) return []
  const row: { changed_fields: string[] } = await qx.selectOne(
    `WITH old AS (
       SELECT number, published_at, is_latest, is_yanked, is_prerelease, licenses
         FROM versions
        WHERE package_id = $(packageId)::bigint AND number = ANY($(numbers)::text[])
     ),
     ins AS (
       INSERT INTO versions (
         package_id, ecosystem, namespace, name, number,
         published_at, is_latest, is_yanked, is_prerelease, licenses, last_synced_at,
         created_at
       )
       SELECT $(packageId)::bigint, 'pypi', p.namespace, p.name, v.num,
              v.pub::timestamptz, v.latest, v.yanked, v.pre,
              CASE WHEN v.lic IS NULL THEN NULL::text[] ELSE ARRAY[v.lic] END,
              NOW(), NOW()
       FROM unnest(
         $(numbers)::text[],
         $(publishedAts)::text[],
         $(isLatests)::bool[],
         $(isYankeds)::bool[],
         $(isPrereleases)::bool[],
         $(licenses)::text[]
       ) AS v(num, pub, latest, yanked, pre, lic)
       CROSS JOIN (SELECT namespace, name FROM packages WHERE id = $(packageId)::bigint) p
       ON CONFLICT (package_id, number) DO UPDATE SET
         published_at   = COALESCE(EXCLUDED.published_at, versions.published_at),
         is_latest      = EXCLUDED.is_latest,
         is_yanked      = EXCLUDED.is_yanked,
         is_prerelease  = EXCLUDED.is_prerelease,
         licenses       = EXCLUDED.licenses,
         last_synced_at = EXCLUDED.last_synced_at
       RETURNING number, published_at, is_latest, is_yanked, is_prerelease, licenses
     )
     SELECT array_remove(ARRAY[
       CASE WHEN bool_or(o.number IS NULL)                                THEN 'versions.number' END,
       CASE WHEN bool_or(o.number IS NULL OR o.published_at IS DISTINCT FROM ins.published_at) THEN 'versions.published_at' END,
       CASE WHEN bool_or(o.is_latest     IS DISTINCT FROM ins.is_latest)     THEN 'versions.is_latest' END,
       CASE WHEN bool_or(o.is_yanked     IS DISTINCT FROM ins.is_yanked)     THEN 'versions.is_yanked' END,
       CASE WHEN bool_or(o.is_prerelease IS DISTINCT FROM ins.is_prerelease) THEN 'versions.is_prerelease' END,
       CASE WHEN bool_or(o.licenses      IS DISTINCT FROM ins.licenses)      THEN 'versions.licenses' END
     ], NULL) AS changed_fields
     FROM ins LEFT JOIN old o ON o.number = ins.number`,
    {
      packageId,
      numbers: versions.map((v) => v.number),
      publishedAts: versions.map((v) => v.publishedAt),
      isLatests: versions.map((v) => v.isLatest),
      isYankeds: versions.map((v) => v.isYanked),
      isPrereleases: versions.map((v) => v.isPrerelease),
      licenses: versions.map((v) => v.license),
    },
  )

  // Clear a stale is_latest on any OTHER version of this package — e.g. a previously-latest
  // version whose files were all deleted, so it is not in this batch and would otherwise keep
  // is_latest = true alongside the new latest.
  const latestNumbers = versions.filter((v) => v.isLatest).map((v) => v.number)
  if (latestNumbers.length > 0) {
    await qx.result(
      `UPDATE versions SET is_latest = false
         WHERE package_id = $(packageId)::bigint
           AND is_latest = true
           AND NOT (number = ANY($(latestNumbers)::text[]))`,
      { packageId, latestNumbers },
    )
  }

  return row.changed_fields
}
