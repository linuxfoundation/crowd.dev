import pgPromise from 'pg-promise'

import { IS_CLOUD_ENV, IS_DEV_ENV } from '@crowd/common'
import { getServiceChildLogger } from '@crowd/logging'

import { DbConnection, DbInstance, IDatabaseConfig } from './types'

const log = getServiceChildLogger('database.connection')

let readDbConfig: IDatabaseConfig | undefined = undefined
export const READ_DB_CONFIG = (): IDatabaseConfig => {
  if (!readDbConfig) {
    readDbConfig = {
      host: process.env.CROWD_DB_READ_HOST,
      port: parseInt(process.env.CROWD_DB_PORT, 10),
      database: process.env.CROWD_DB_DATABASE,
      user: process.env.CROWD_DB_USERNAME,
      password: process.env.CROWD_DB_PASSWORD,
    }
  }

  return readDbConfig
}

let writeDbConfig: IDatabaseConfig | undefined = undefined
export const WRITE_DB_CONFIG = (): IDatabaseConfig => {
  if (!writeDbConfig) {
    writeDbConfig = {
      host: process.env.CROWD_DB_WRITE_HOST,
      port: parseInt(process.env.CROWD_DB_PORT, 10),
      database: process.env.CROWD_DB_DATABASE,
      user: process.env.CROWD_DB_USERNAME,
      password: process.env.CROWD_DB_PASSWORD,
    }
  }

  return writeDbConfig
}

const KEEP_ALIVE_INITIAL_DELAY_MS = 60 * 1000
const QUERY_READ_TIMEOUT_MESSAGE = 'Query read timeout'
const MAX_TIMEOUT_MS = 2147483647

type TimedOutClient = pgPromise.IEventContext['client'] & { $connectionError?: Error }

const parseTimeoutMs = (name: string, value: number | string | undefined): number | undefined => {
  if (value === undefined || value === null || value === '') {
    return undefined
  }

  const parsed = Number(value)
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > MAX_TIMEOUT_MS) {
    throw new Error(`Invalid database ${name}: ${value} (expected 1..${MAX_TIMEOUT_MS} ms)`)
  }

  return parsed
}

// pg rejects on query_timeout but keeps the client busy, so later queries on it would hang:
// https://github.com/brianc/node-postgres/blob/pg%408.11.5/packages/pg/lib/client.js#L532-L555
const closeTimedOutClient = (client: TimedOutClient): void => {
  client.$connectionError = client.$connectionError || new Error(QUERY_READ_TIMEOUT_MESSAGE)
  // pg-promise blocks client.end while the client is checked out (lib/connect.js lockClientEnd)
  Object.getPrototypeOf(client)
    .end.call(client)
    .catch((err: Error) => log.warn(err, 'Failed to close a timed out PostgreSQL client!'))
}

let dbInstance: DbInstance | undefined

export const getDbInstance = (): DbInstance => {
  if (dbInstance) {
    return dbInstance
  }

  log.trace('Creating database library instance!')

  dbInstance = pgPromise({
    // tslint:disable-next-line:max-line-length
    // see https://stackoverflow.com/questions/36120435/verify-database-connection-with-pg-promise-when-starting-an-app
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    async error(err: any, e: pgPromise.IEventContext): Promise<void> {
      if (e.cn) {
        log.fatal(err, { cn: e.cn }, 'PostgreSQL connection error. Stopping process')
        // logs don't have flush:
        await new Promise((resolve) => setTimeout(resolve, 100))
        process.nextTick(() => process.exit())
      }

      if (e.query) {
        log.error(err, { query: e.query, params: e.params }, 'Error executing a PostgreSQL query!')
      }

      if (err?.message === QUERY_READ_TIMEOUT_MESSAGE && e.client) {
        closeTimedOutClient(e.client)
      }
    },
    query(e) {
      log.debug({ query: e.query, params: e.params }, 'Executing PostgreSQL query')
    },
  })

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  if ((dbInstance.pg as any).usingSequelize) {
    return dbInstance
  }

  // timestamp
  dbInstance.pg.types.setTypeParser(1114, (s) => s)

  // timestamp with timezone
  dbInstance.pg.types.setTypeParser(1184, (s) => s)

  // numeric
  dbInstance.pg.types.setTypeParser(1700, (s) => parseFloat(s))

  // int4
  dbInstance.pg.types.setTypeParser(23, (s) => parseInt(s, 10))

  return dbInstance
}

const dbConnection: Record<string, DbConnection | undefined> = {}
const dbConnectionTimeouts: Record<string, string> = {}
const warnedTimeouts = new Set<string>()

export const getDbConnection = async (
  config: IDatabaseConfig,
  maxPoolSize?: number,
  idleTimeoutMillis?: number,
): Promise<DbConnection> => {
  const { queryTimeoutMs, statementTimeoutMs, lockTimeoutMs, connectionTimeoutMs, ...connection } =
    config
  const timeouts = {
    query_timeout: parseTimeoutMs('queryTimeoutMs', queryTimeoutMs),
    statement_timeout: parseTimeoutMs('statementTimeoutMs', statementTimeoutMs),
    lock_timeout: parseTimeoutMs('lockTimeoutMs', lockTimeoutMs),
    connectionTimeoutMillis: parseTimeoutMs('connectionTimeoutMs', connectionTimeoutMs),
  }

  const cacheKey = `${config.host}:${config.database}`
  if (dbConnection[cacheKey]) {
    const requested = JSON.stringify(timeouts)
    if (dbConnectionTimeouts[cacheKey] !== requested && !warnedTimeouts.has(cacheKey + requested)) {
      warnedTimeouts.add(cacheKey + requested)
      log.warn(
        { database: config.database, host: config.host, requested: timeouts },
        'Reusing a database connection created with different timeouts!',
      )
    }
    return dbConnection[cacheKey]
  }

  log.info(
    { database: config.database, host: config.host, port: config.port },
    'Connecting to database!',
  )

  const dbInstance = getDbInstance()

  if (Object.values(timeouts).some((value) => value !== undefined)) {
    log.info(timeouts, 'Database connection timeouts!')
  }

  const client = dbInstance({
    ...connection,
    ...Object.fromEntries(Object.entries(timeouts).filter(([, value]) => value !== undefined)),
    ssl:
      IS_CLOUD_ENV || process.env.DEBUG_PGDB_SSL === 'true'
        ? {
            rejectUnauthorized: false,
          }
        : false,
    max: maxPoolSize || (IS_DEV_ENV ? 5 : 20),
    idleTimeoutMillis: idleTimeoutMillis !== undefined ? idleTimeoutMillis : 10000,
    keepAlive: true,
    keepAliveInitialDelayMillis: KEEP_ALIVE_INITIAL_DELAY_MS,
    application_name: process.env.SERVICE ? `${process.env.SERVICE}-pg` : 'unknown-app=pg',
  })

  const profile = process.env['CROWD_POSTGRESQL_PROFILE_QUERIES'] !== undefined
  const minQueryDurationMs = Number(
    process.env['CROWD_POSTGRESQL_PROFILE_QUERIES_MIN_DURATION'] || 0,
  )

  const oldQuery = client.query
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  ;(client as any).query = async (query, options, ...args) => {
    // milliseconds
    const start = performance.now()
    try {
      const result = await oldQuery.apply(client, [query, options, ...args])
      return result
    } finally {
      const duration = performance.now() - start
      if (profile && duration >= minQueryDurationMs) {
        const durationSeconds = duration / 1000.0
        log.warn(
          { durationSeconds: durationSeconds.toFixed(2), query, values: options },
          'PostgreSQL query duration profiling!',
        )
      }
    }
  }

  dbConnection[cacheKey] = client
  dbConnectionTimeouts[cacheKey] = JSON.stringify(timeouts)

  return client
}
