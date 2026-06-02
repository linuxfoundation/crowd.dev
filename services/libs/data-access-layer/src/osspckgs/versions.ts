import { QueryExecutor } from '../queryExecutor'

import { IDbVersionUpsert } from './types'

/**
 * Bulk-upserts a list of versions for a single package.
 * Uses UNNEST arrays to avoid N individual round-trips.
 * On conflict (package_id, number) updates is_latest, is_prerelease, and
 * license (never overwrites an existing license with NULL).
 * Returns the list of fields that actually changed across all versions.
 */
export async function upsertVersionsBatch(
  qx: QueryExecutor,
  versions: IDbVersionUpsert[],
): Promise<string[]> {
  if (versions.length === 0) return []

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
      SELECT number, is_latest, is_prerelease, license
        FROM versions
       WHERE package_id = $(packageId)::bigint AND number = ANY($(numbers)::text[])
    ),
    ins AS (
      INSERT INTO versions (package_id, ecosystem, name, number, is_latest, is_prerelease, license, last_synced_at)
      SELECT
        UNNEST($(packageIds)::bigint[]),
        UNNEST($(ecosystems)::text[]),
        UNNEST($(names)::text[]),
        UNNEST($(numbers)::text[]),
        UNNEST($(isLatests)::bool[]),
        UNNEST($(isPreleases)::bool[]),
        UNNEST($(licenses)::text[]),
        NOW()
      ON CONFLICT (package_id, number) DO UPDATE SET
        is_latest      = EXCLUDED.is_latest,
        is_prerelease  = EXCLUDED.is_prerelease,
        license        = COALESCE(EXCLUDED.license, versions.license),
        last_synced_at = NOW()
      RETURNING number, is_latest, is_prerelease, license
    )
    SELECT array_remove(ARRAY[
      CASE WHEN bool_or(o.number IS NULL)                                          THEN 'versions.number' END,
      CASE WHEN bool_or(o.is_latest     IS DISTINCT FROM ins.is_latest)            THEN 'versions.is_latest' END,
      CASE WHEN bool_or(o.is_prerelease IS DISTINCT FROM ins.is_prerelease)        THEN 'versions.is_prerelease' END,
      CASE WHEN bool_or(o.license       IS DISTINCT FROM ins.license)              THEN 'versions.license' END
    ], NULL) AS changed_fields
    FROM ins LEFT JOIN old o ON o.number = ins.number
    `,
    {
      packageId: versions[0].packageId,
      packageIds: versions.map((v) => v.packageId),
      ecosystems: versions.map((v) => v.ecosystem),
      names: versions.map((v) => v.name),
      numbers: versions.map((v) => v.number),
      isLatests: versions.map((v) => v.isLatest),
      isPreleases: versions.map((v) => v.isPrerelease),
      licenses: versions.map((v) => v.license),
    },
  )
  return row.changed_fields
}
