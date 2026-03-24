import { Pool } from 'pg'

import { Config } from './config'

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

export async function fetchRepositoryUrls(config: Config): Promise<string[]> {
  const client = getPool(config)

  try {
    const result = await client.query(
      `SELECT url FROM public.repositories
       WHERE 
         "deletedAt" IS NULL AND
         (starts_with(url, 'https://github.com/') OR starts_with(url, 'https://gitlab.com/')) AND
         ("lastArchivedCheckAt" IS NULL OR "lastArchivedCheckAt" < NOW() - INTERVAL '3 days')
       ORDER BY "lastArchivedCheckAt" NULLS FIRST
       LIMIT $1`,
      [config.BatchSize],
    )

    return result.rows.map((row) => row.url)
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
