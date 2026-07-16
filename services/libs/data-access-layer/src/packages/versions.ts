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

export interface PackagistVersionInput {
  number: string
  publishedAt: string | null
  isLatest: boolean
  isPrerelease: boolean
  license: string | null
}

export async function upsertPackagistVersions(
  qx: QueryExecutor,
  packageId: string,
  versions: PackagistVersionInput[],
  latestNumber: string | null,
): Promise<{ changedFields: string[]; versionIds: Array<{ number: string; id: string }> }> {
  if (versions.length === 0) return { changedFields: [], versionIds: [] }

  const row: {
    changed_fields: string[]
    version_ids: Array<{ number: string; id: string }>
  } = await qx.selectOne(
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
       SELECT $(packageId)::bigint, 'packagist', p.namespace, p.name, v.num,
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
         published_at   = COALESCE(EXCLUDED.published_at, versions.published_at),
         is_latest      = EXCLUDED.is_latest,
         is_prerelease  = EXCLUDED.is_prerelease,
         licenses       = EXCLUDED.licenses,
         last_synced_at = EXCLUDED.last_synced_at
       RETURNING number, published_at, is_latest, is_prerelease, licenses, id::text AS id
     )
     SELECT array_remove(ARRAY[
       CASE WHEN bool_or(o.number IS NULL)                                THEN 'versions.number' END,
       CASE WHEN bool_or(o.number IS NULL OR o.published_at IS DISTINCT FROM ins.published_at) THEN 'versions.published_at' END,
       CASE WHEN bool_or(o.is_latest     IS DISTINCT FROM ins.is_latest)     THEN 'versions.is_latest' END,
       CASE WHEN bool_or(o.is_prerelease IS DISTINCT FROM ins.is_prerelease) THEN 'versions.is_prerelease' END,
       CASE WHEN bool_or(o.licenses      IS DISTINCT FROM ins.licenses)      THEN 'versions.licenses' END
     ], NULL) AS changed_fields,
            json_agg(json_build_object('number', ins.number, 'id', ins.id))::jsonb AS version_ids
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

  // Parse the version_ids from JSONB
  const versionIds = (row.version_ids as Array<{ number: string; id: string }>) || []

  // Clear a stale is_latest on every OTHER version of this package. Anchored on the declared
  // latest (latestNumber) — NOT on what's in this batch — so a previously-latest version whose
  // records were updated differently can't keep is_latest = true alongside the new latest. When
  // no latest is known, leave flags untouched rather than wipe all.
  if (latestNumber != null) {
    await qx.result(
      `UPDATE versions SET is_latest = false
         WHERE package_id = $(packageId)::bigint
           AND is_latest = true
           AND number <> $(latestNumber)`,
      { packageId, latestNumber },
    )
  }

  return { changedFields: row.changed_fields, versionIds }
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
  latestNumber: string | null,
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

  // Clear a stale is_latest on every OTHER version of this package. Anchored on the declared
  // latest (info.version) — NOT on what's in this batch — so a previously-latest version whose
  // files were all deleted (and is therefore omitted from the batch) can't keep is_latest = true
  // alongside the new latest. When no latest is known, leave flags untouched rather than wipe all.
  if (latestNumber != null) {
    await qx.result(
      `UPDATE versions SET is_latest = false
         WHERE package_id = $(packageId)::bigint
           AND is_latest = true
           AND number <> $(latestNumber)`,
      { packageId, latestNumber },
    )
  }

  return row.changed_fields
}
