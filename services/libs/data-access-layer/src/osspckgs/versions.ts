import { QueryExecutor } from '../queryExecutor'

import { IDbVersionUpsert } from './types'

/**
 * Bulk-upserts a list of versions for a single package. All elements must share
 * the same packageId — throws otherwise (the change-detection logic assumes it).
 * Uses UNNEST arrays to avoid N individual round-trips.
 * On conflict (package_id, number) updates is_latest, is_prerelease, and
 * licenses (never overwrites an existing licenses array with NULL).
 * The per-version `license` input is stored as a single-element text[] in the
 * `licenses` column (the schema is an array to match packages.licenses).
 * Returns the list of fields that actually changed across all versions.
 */
export async function upsertVersionsBatch(
  qx: QueryExecutor,
  versions: IDbVersionUpsert[],
): Promise<string[]> {
  if (versions.length === 0) return []

  // This function operates on a single package: `old` reads by a scalar packageId and
  // the changed-fields join keys on `number` alone, so mixing packageIds would silently
  // corrupt the result. Enforce the invariant rather than rely on the caller.
  const packageId = versions[0].packageId
  if (versions.some((v) => v.packageId !== packageId)) {
    throw new Error('upsertVersionsBatch: all versions must belong to the same package')
  }

  // maven-metadata.xml sometimes contains duplicate version strings — deduplicate
  // by number before inserting to avoid "ON CONFLICT DO UPDATE command cannot affect
  // row a second time" from PostgreSQL
  const seen = new Set<string>()
  versions = versions.filter((v) => {
    if (seen.has(v.number)) return false
    seen.add(v.number)
    return true
  })

  const row: { changed_fields: string[] } = await qx.selectOne(
    `
    WITH old AS (
      SELECT number, is_latest, is_prerelease, licenses, published_at
        FROM versions
       WHERE package_id = $(packageId)::bigint AND number = ANY($(numbers)::text[])
    ),
    ins AS (
      INSERT INTO versions (package_id, ecosystem, namespace, name, number, is_latest, is_prerelease, licenses, published_at, last_synced_at, created_at)
      SELECT
        $(packageId)::bigint, t.ecosystem, t.namespace, t.name, t.number, t.is_latest, t.is_prerelease,
        CASE WHEN t.license IS NULL THEN NULL ELSE ARRAY[t.license] END,
        t.published_at::timestamptz,
        NOW(), NOW()
      FROM UNNEST(
        $(ecosystems)::text[],
        $(namespaces)::text[],
        $(names)::text[],
        $(numbers)::text[],
        $(isLatests)::bool[],
        $(isPreleases)::bool[],
        $(licenses)::text[],
        $(publishedAts)::text[]
      ) AS t(ecosystem, namespace, name, number, is_latest, is_prerelease, license, published_at)
      ON CONFLICT (package_id, number) DO UPDATE SET
        namespace      = COALESCE(EXCLUDED.namespace, versions.namespace),
        is_latest      = EXCLUDED.is_latest,
        is_prerelease  = EXCLUDED.is_prerelease,
        licenses       = COALESCE(EXCLUDED.licenses, versions.licenses),
        published_at   = COALESCE(EXCLUDED.published_at, versions.published_at),
        last_synced_at = NOW()
      RETURNING number, is_latest, is_prerelease, licenses, published_at
    )
    SELECT array_remove(ARRAY[
      CASE WHEN bool_or(o.number IS NULL)                                          THEN 'versions.number' END,
      CASE WHEN bool_or(o.is_latest     IS DISTINCT FROM ins.is_latest)            THEN 'versions.is_latest' END,
      CASE WHEN bool_or(o.is_prerelease IS DISTINCT FROM ins.is_prerelease)        THEN 'versions.is_prerelease' END,
      CASE WHEN bool_or(o.licenses      IS DISTINCT FROM ins.licenses)             THEN 'versions.licenses' END,
      CASE WHEN bool_or(o.published_at  IS DISTINCT FROM ins.published_at)         THEN 'versions.published_at' END
    ], NULL) AS changed_fields
    FROM ins LEFT JOIN old o ON o.number = ins.number
`,
    {
      packageId,
      ecosystems: versions.map((v) => v.ecosystem),
      namespaces: versions.map((v) => v.namespace),
      names: versions.map((v) => v.name),
      numbers: versions.map((v) => v.number),
      isLatests: versions.map((v) => v.isLatest),
      isPreleases: versions.map((v) => v.isPrerelease),
      licenses: versions.map((v) => v.license),
      publishedAts: versions.map((v) => (v.publishedAt ? v.publishedAt.toISOString() : null)),
    },
  )
  return row.changed_fields
}
