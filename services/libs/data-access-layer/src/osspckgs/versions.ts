import { QueryExecutor } from '../queryExecutor'

import { IDbVersionUpsert } from './types'

/**
 * Bulk-upserts a list of versions for a single package.
 * Uses UNNEST arrays to avoid N individual round-trips.
 * On conflict (package_id, number) updates is_latest, is_prerelease, and
 * license (never overwrites an existing license with NULL).
 */
export async function upsertVersionsBatch(
  qx: QueryExecutor,
  versions: IDbVersionUpsert[],
): Promise<void> {
  if (versions.length === 0) return

  // maven-metadata.xml sometimes contains duplicate version strings — deduplicate
  // by number before inserting to avoid "ON CONFLICT DO UPDATE command cannot affect
  // row a second time" from PostgreSQL
  const seen = new Set<string>()
  versions = versions.filter((v) => {
    if (seen.has(v.number)) return false
    seen.add(v.number)
    return true
  })

  await qx.result(
    `
    INSERT INTO versions (package_id, ecosystem, number, is_latest, is_prerelease, license, last_synced_at)
    SELECT
      UNNEST($(packageIds)::bigint[]),
      UNNEST($(ecosystems)::text[]),
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
    `,
    {
      packageIds: versions.map((v) => v.packageId),
      ecosystems: versions.map((v) => v.ecosystem),
      numbers: versions.map((v) => v.number),
      isLatests: versions.map((v) => v.isLatest),
      isPreleases: versions.map((v) => v.isPrerelease),
      licenses: versions.map((v) => v.license),
    },
  )
}
