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
): Promise<void> {
  if (versions.length === 0) return
  await qx.result(
    `INSERT INTO versions (package_id, ecosystem, number, published_at, is_latest, is_prerelease, license, last_synced_at)
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
       last_synced_at = EXCLUDED.last_synced_at`,
    {
      packageId,
      numbers: versions.map((v) => v.number),
      publishedAts: versions.map((v) => v.publishedAt),
      isLatests: versions.map((v) => v.isLatest),
      isPreleases: versions.map((v) => v.isPrerelease),
      licenses: versions.map((v) => v.license),
    },
  )
}
