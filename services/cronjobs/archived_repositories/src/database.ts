import { Pool } from 'pg'

import { Config } from './config'
import { RepositoryToCheck } from './types'

let pool: Pool | null = null

function getPool(config: Config): Pool {
  // If the pool is already created, return it
  if (pool) {
    return pool
  }

  pool = new Pool({
    host: config.PostgresHost,
    port: config.PostgresPort,
    database: config.PostgresDBName,
    user: config.PostgresUsername,
    password: config.PostgresPassword,
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
    maxLifetimeSeconds: 60,
    ssl: {
      rejectUnauthorized: false,
    },
  })

  return pool
}

export async function fetchRepositoryUrls(config: Config): Promise<RepositoryToCheck[]> {
  const client = getPool(config)

  try {
    // Gerrit instances are self-hosted, so Gerrit repositories are found through their integration
    // rather than by host.
    const result = await client.query(
      `SELECT r.url, gi.settings->'remote'->>'orgURL' AS "gerritBaseUrl"
       FROM public.repositories r
       LEFT JOIN public.integrations gi
         ON gi.id = r."sourceIntegrationId" AND gi.platform = 'gerrit' AND gi."deletedAt" IS NULL
       WHERE
         r."deletedAt" IS NULL AND
         (
           starts_with(r.url, 'https://github.com/') OR
           starts_with(r.url, 'https://gitlab.com/') OR
           gi.settings->'remote'->>'orgURL' IS NOT NULL
         ) AND
         (r."lastArchivedCheckAt" IS NULL OR r."lastArchivedCheckAt" < NOW() - INTERVAL '3 days')
       ORDER BY r."lastArchivedCheckAt" NULLS FIRST
       LIMIT $1`,
      [config.BatchSize],
    )

    return result.rows.map((row) => ({ url: row.url, gerritBaseUrl: row.gerritBaseUrl ?? null }))
  } catch (error) {
    console.error('Error fetching repository URLs:', error)
    throw error
  }
}

export async function updateRepositoryStatus(
  repository: string,
  isArchived: boolean,
  isExcluded: boolean,
  config: Config,
): Promise<void> {
  const client = getPool(config)

  try {
    await client.query(
      `UPDATE public.repositories 
       SET "archived" = $1, "excluded" = $2, "lastArchivedCheckAt" = NOW(), "updatedAt" = NOW()
       WHERE "url" = $3 AND "deletedAt" IS NULL`,
      [isArchived, isExcluded, repository],
    )
  } catch (error) {
    console.error(`Error updating repository status for ${repository}:`, error)
    throw error
  }
}

export async function closeConnection(): Promise<void> {
  if (pool) {
    await pool.end()
    pool = null
  }
}
