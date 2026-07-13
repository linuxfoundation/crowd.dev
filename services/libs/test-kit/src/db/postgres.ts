import pgPromise from 'pg-promise'

import { DEFAULT_TENANT_ID, IS_TEST_ENV } from '@crowd/common'
import { type DbConnection, type DbInstance, type IDatabaseConfig } from '@crowd/database'
import type { QueryExecutor } from '@crowd/database'

type TestPostgres = {
  host: string
  port: number
  user: string
  password: string
}

/**
 * Creates an isolated, dynamic test database cloned from the template database
 * for the current Vitest worker pool, and handles its lifecycle cleanup.
 */
export async function openTestWorkerDatabase(): Promise<{
  db: DbConnection
  cleanup: () => Promise<void>
}> {
  const postgres = getTestPostgres()

  const poolId = process.env.VITEST_POOL_ID ?? '0'
  const name = `test_${poolId.replace(/[^a-z0-9_]/gi, '_').toLowerCase()}`

  await withCatalogDb(postgres, async (catalog) => {
    await catalog.result(`SELECT pg_advisory_lock(hashtext('cdp-test-template-clone'))`)
    try {
      await dropDatabase(catalog, name)
      await catalog.result(
        `SELECT pg_terminate_backend(pid)
         FROM pg_stat_activity
         WHERE datname = 'test_template' AND pid <> pg_backend_pid()`,
      )
      await catalog.none(`CREATE DATABASE ${quoteIdent(name)} WITH TEMPLATE "test_template"`)
    } finally {
      await catalog.result(`SELECT pg_advisory_unlock(hashtext('cdp-test-template-clone'))`)
    }
  })

  const db = connectTestDb({
    host: postgres.host,
    port: postgres.port,
    user: postgres.user,
    password: postgres.password,
    database: name,
  })

  return {
    db,
    async cleanup() {
      await db.$pool.end()
      await withCatalogDb(postgres, (catalog) => dropDatabase(catalog, name))
    },
  }
}

/**
 * Truncates all public tables in the worker database.
 * @throws {Error} If executed against a non-test database name.
 */
export async function resetTestDatabase(qx: QueryExecutor): Promise<void> {
  const { name } = await qx.selectOne('SELECT current_database() AS name')

  if (!/^test_[a-z0-9_]+$/.test(name)) {
    throw new Error(`Expected worker test database (got ${name})`)
  }

  await qx.selectNone(`
    DO $$
    DECLARE tables text;
    BEGIN
      SELECT string_agg(format('%I.%I', schemaname, tablename), ', ')
      INTO tables
      FROM pg_tables
      WHERE schemaname = 'public';

      IF tables IS NOT NULL THEN
        EXECUTE 'TRUNCATE TABLE ' || tables || ' RESTART IDENTITY CASCADE';
      END IF;
    END $$;
  `)
}

/**
 * Seeds the default tenant into the database after a table reset.
 */
export async function seedTestBaseline(qx: QueryExecutor): Promise<void> {
  await qx.result(
    `
    INSERT INTO tenants (id, name, url, plan, "createdAt", "updatedAt")
    VALUES ($(id), 'Default', 'default', 'Essential', NOW(), NOW())
    ON CONFLICT (id) DO NOTHING
    `,
    { id: DEFAULT_TENANT_ID },
  )
}

/**
 * Executes an operation scoped to a short-lived catalog connection (`postgres` DB),
 * ensuring the connection pool is cleanly terminated afterward.
 */
async function withCatalogDb<T>(
  endpoint: TestPostgres,
  fn: (catalog: DbConnection) => Promise<T>,
): Promise<T> {
  const catalog = connectTestDb({
    host: endpoint.host,
    port: endpoint.port,
    user: endpoint.user,
    password: endpoint.password,
    database: 'postgres',
  })
  try {
    return await fn(catalog)
  } finally {
    await catalog.$pool.end()
  }
}

/**
 * Forcefully drops a target database by terminating active connections first.
 */
async function dropDatabase(catalog: DbConnection, name: string): Promise<void> {
  if (name !== 'test_template' && !/^test_[a-z0-9_]+$/.test(name)) {
    throw new Error(`Not a test database: ${name}`)
  }

  await catalog.result(
    `SELECT pg_terminate_backend(pid)
     FROM pg_stat_activity
     WHERE datname = $(database) AND pid <> pg_backend_pid()`,
    { database: name },
  )
  await catalog.none(`DROP DATABASE IF EXISTS ${quoteIdent(name)}`)
}

/**
 * Resolves and validates local test database credentials from the environment.
 */
function getTestPostgres(): TestPostgres {
  if (!IS_TEST_ENV) {
    throw new Error(`Expected NODE_ENV=test (got ${process.env.NODE_ENV ?? 'unset'})`)
  }

  const { DB_HOST: host, DB_PORT, DB_USER: user, DB_PASSWORD: password } = process.env

  if (!host || !DB_PORT || !user || !password) {
    throw new Error('Missing required database environment variables')
  }

  const port = Number(DB_PORT)

  if (!Number.isInteger(port) || port <= 0 || port > 65535) {
    throw new Error(`Expected valid DB_PORT (got ${DB_PORT})`)
  }

  if (!['localhost', '127.0.0.1', '::1'].includes(host)) {
    throw new Error(`Expected local DB_HOST (got ${host})`)
  }

  return { host, port, user, password }
}

let testPgInstance: DbInstance | undefined

/** Test-scoped pg-promise main instance (no prod error/query hooks). */
function createTestPgPromise(): DbInstance {
  if (testPgInstance) {
    return testPgInstance
  }

  testPgInstance = pgPromise({})

  // Keep in sync with getDbInstance()
  testPgInstance.pg.types.setTypeParser(1114, (s) => s)
  testPgInstance.pg.types.setTypeParser(1184, (s) => s)
  testPgInstance.pg.types.setTypeParser(1700, (s) => parseFloat(s))
  testPgInstance.pg.types.setTypeParser(23, (s) => parseInt(s, 10))

  return testPgInstance
}

function connectTestDb(config: IDatabaseConfig): DbConnection {
  return createTestPgPromise()({
    ...config,
    ssl: false,
    max: 5,
    idleTimeoutMillis: 10_000,
    application_name: 'cdp-test',
  })
}

/**
 * Standard PostgreSQL identifier wrapping for variable database names.
 */
function quoteIdent(name: string): string {
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
    throw new Error(`Invalid database name: ${name}`)
  }
  return `"${name}"`
}
