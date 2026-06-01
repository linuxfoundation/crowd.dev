import { QueryExecutor } from '../queryExecutor'

export interface NpmVersionInput {
  number: string
  publishedAt: string | null
  isLatest: boolean
  isPrerelease: boolean
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
       SELECT number, is_latest, is_prerelease, license
         FROM versions
        WHERE package_id = $(packageId)::bigint AND number = ANY($(numbers)::text[])
     ),
     ins AS (
       INSERT INTO versions (package_id, ecosystem, number, published_at, is_latest, is_prerelease, license, last_synced_at)
       SELECT $(packageId)::bigint, 'npm', v.num, v.pub::timestamptz, v.latest, v.pre, v.lic, NOW()
       FROM unnest(
         $(numbers)::text[],
         $(publishedAts)::text[],
         $(isLatests)::bool[],
         $(isPreleases)::bool[],
         $(licenses)::text[]
       ) AS v(num, pub, latest, pre, lic)
       ON CONFLICT (package_id, number) DO UPDATE SET
         is_latest      = EXCLUDED.is_latest,
         is_prerelease  = EXCLUDED.is_prerelease,
         license        = EXCLUDED.license,
         last_synced_at = EXCLUDED.last_synced_at
       RETURNING number, is_latest, is_prerelease, license
     )
     SELECT array_remove(ARRAY[
       CASE WHEN bool_or(o.number IS NULL)                                THEN 'versions.number' END,
       CASE WHEN bool_or(o.number IS NULL)                                THEN 'versions.published_at' END,
       CASE WHEN bool_or(o.is_latest     IS DISTINCT FROM ins.is_latest)     THEN 'versions.is_latest' END,
       CASE WHEN bool_or(o.is_prerelease IS DISTINCT FROM ins.is_prerelease) THEN 'versions.is_prerelease' END,
       CASE WHEN bool_or(o.license       IS DISTINCT FROM ins.license)       THEN 'versions.license' END
     ], NULL) AS changed_fields
     FROM ins LEFT JOIN old o ON o.number = ins.number`,
    {
      packageId,
      numbers: versions.map((v) => v.number),
      publishedAts: versions.map((v) => v.publishedAt),
      isLatests: versions.map((v) => v.isLatest),
      isPreleases: versions.map((v) => v.isPrerelease),
      licenses: versions.map((v) => v.license),
    },
  )
  return row.changed_fields
}
