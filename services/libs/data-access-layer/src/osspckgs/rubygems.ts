import { QueryExecutor } from '../queryExecutor'

export type RubyGemsPackageToSync = {
  id: number
  purl: string
  name: string
  latestVersion: string | null
}

const RUBYGEMS_WORKER_OUTCOMES = ['rubygems-registry', 'rubygems_not_found', 'rubygems_error']

export async function listRubyGemsPackagesToSync(
  qx: QueryExecutor,
  options: { limit: number },
): Promise<RubyGemsPackageToSync[]> {
  const { limit } = options
  return qx.select(
    `
    SELECT
      p.id,
      p.purl,
      p.name,
      p.latest_version AS "latestVersion"
    FROM packages p
    WHERE
      p.ecosystem = 'rubygems'
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
    { limit, workerOutcomes: RUBYGEMS_WORKER_OUTCOMES },
  )
}

export type RubyGemsCriticalPackageToSync = {
  id: number
  purl: string
  name: string
}

export async function listRubyGemsCriticalPackagesToSync(
  qx: QueryExecutor,
  options: { limit: number },
): Promise<RubyGemsCriticalPackageToSync[]> {
  const { limit } = options
  return qx.select(
    `
    SELECT p.id, p.purl, p.name
    FROM packages p
    LEFT JOIN LATERAL (
      SELECT MAX(v.last_synced_at) AS last_synced
      FROM versions v WHERE v.package_id = p.id
    ) v ON true
    WHERE p.ecosystem = 'rubygems'
      AND p.is_critical
      AND (v.last_synced IS NULL OR v.last_synced < NOW() - INTERVAL '1 day')
    ORDER BY p.dependent_count DESC NULLS LAST, p.id ASC
    LIMIT $(limit)
    `,
    { limit },
  )
}
