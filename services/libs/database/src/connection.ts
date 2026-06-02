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

export const formatQuery = (query: string, values: Record<string, unknown>): string => {
  if (!dbInstance) {
    throw new Error('Database instance not initialized!')
  }

  return dbInstance.as.format(query, values)
}

const dbConnection: Record<string, DbConnection | undefined> = {}

export const getDbConnection = async (
  config: IDatabaseConfig,
  maxPoolSize?: number,
  idleTimeoutMillis?: number,
): Promise<DbConnection> => {
  const cacheKey = `${config.host}:${config.database}`
  if (dbConnection[cacheKey]) {
    return dbConnection[cacheKey]
  }

  log.info(
    { database: config.database, host: config.host, port: config.port },
    'Connecting to database!',
  )

  const dbInstance = getDbInstance()

  const client = dbInstance({
    ...config,
    ssl:
      IS_CLOUD_ENV || process.env.DEBUG_PGDB_SSL === 'true'
        ? {
            rejectUnauthorized: false,
          }
        : false,
    max: maxPoolSize || (IS_DEV_ENV ? 5 : 20),
    idleTimeoutMillis: idleTimeoutMillis !== undefined ? idleTimeoutMillis : 10000,
    // query_timeout: 30000,
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

  return client
}
