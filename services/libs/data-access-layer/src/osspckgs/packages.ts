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
 * Returns a page of Maven packages from packages_universe that either have no
 * corresponding entry in `packages` yet, or whose `packages.last_synced_at` is
 * older than the given cutoff (defaults to 7 days).
 *
 * Ordered by rank_in_ecosystem ASC (most critical first), unranked last.
 */
export async function listMavenPackagesToEnrich(
  qx: QueryExecutor,
  options: { limit: number; offset: number; staleDays?: number },
): Promise<Pick<IDbPackageUniverse, 'id' | 'purl' | 'namespace' | 'name'>[]> {
  const { limit, offset, staleDays = 7 } = options

  return qx.select(
    `
    SELECT
      pu.id,
      pu.purl,
      pu.namespace,
      pu.name
    FROM packages_universe pu
    LEFT JOIN packages p ON p.purl = pu.purl
    WHERE
      pu.ecosystem = 'maven'
      AND pu.namespace IS NOT NULL
      AND (
        p.id IS NULL
        OR p.last_synced_at < NOW() - ($(staleDays) || ' days')::interval
      )
    ORDER BY
      pu.rank_in_ecosystem ASC NULLS LAST,
      pu.id ASC
    LIMIT $(limit) OFFSET $(offset)
    `,
    { limit, offset, staleDays },
  )
}

// ─── packages upsert ──────────────────────────────────────────────────────────

/**
 * Inserts or updates a row in `packages`.
 * Returns the id of the upserted row.
 */
export async function upsertPackage(qx: QueryExecutor, item: IDbPackageUpsert): Promise<number> {
  const row = await qx.selectOne(
    `
    INSERT INTO packages (
      purl,
      ecosystem,
      namespace,
      name,
      description,
      homepage,
      declared_repository_url,
      licenses,
      licenses_raw,
      latest_version,
      ingestion_source,
      last_synced_at
    ) VALUES (
      $(purl),
      $(ecosystem),
      $(namespace),
      $(name),
      $(description),
      $(homepage),
      $(declaredRepositoryUrl),
      $(licenses)::text[],
      $(licensesRaw),
      $(latestVersion),
      $(ingestionSource),
      NOW()
    )
    ON CONFLICT (purl) DO UPDATE SET
      description             = EXCLUDED.description,
      homepage                = EXCLUDED.homepage,
      declared_repository_url = EXCLUDED.declared_repository_url,
      licenses                = EXCLUDED.licenses,
      licenses_raw            = EXCLUDED.licenses_raw,
      latest_version          = COALESCE(EXCLUDED.latest_version, packages.latest_version),
      ingestion_source        = EXCLUDED.ingestion_source,
      last_synced_at          = NOW()
    RETURNING id
    `,
    item,
  )
  return row.id as number
}
