/* eslint-disable @typescript-eslint/no-explicit-any */
import crypto from 'crypto'
import Snowflake from 'snowflake-sdk'

import { Logger, getChildLogger } from '@crowd/logging'

Snowflake.configure({
  logLevel: 'ERROR',
  logFilePath: 'STDOUT',
  additionalLogToConsole: false,
})

interface ISnowflakeBaseConfig {
  account: string
  username: string
  database: string
  warehouse: string
  role?: string
  maxConnections?: number
  minConnections?: number
  parentLog: Logger
}

interface ISnowflakeKeyPairConfig extends ISnowflakeBaseConfig {
  privateKeyString: string | Buffer
  privateKeyPassphrase?: string
  token?: never
}

interface ISnowflakeTokenConfig extends ISnowflakeBaseConfig {
  token: string
  privateKeyString?: never
  privateKeyPassphrase?: never
}

export type ISnowflakeConfig = ISnowflakeKeyPairConfig | ISnowflakeTokenConfig

export class SnowflakeClient {
  private pool: Snowflake.Pool<Snowflake.Connection>
  private log: Logger

  constructor(config: ISnowflakeConfig) {
    const {
      account,
      username,
      database,
      warehouse,
      role,
      maxConnections = 5,
      minConnections = 1,
      parentLog,
    } = config

    this.log = getChildLogger('SnowflakeClient', parentLog)

    let connectionOptions: any

    if ('token' in config && config.token) {
      // Programmatic Access Token (PAT) authentication
      this.log.info(
        { account, database, warehouse, username },
        'Using programmatic access token authentication',
      )
      connectionOptions = {
        account,
        username,
        database,
        warehouse,
        role,
        authenticator: 'PROGRAMMATIC_ACCESS_TOKEN',
        token: config.token,
      }
    } else if ('privateKeyString' in config && config.privateKeyString) {
      // Key-pair authentication
      let privateKey: string | Buffer
      try {
        const formattedKey = config.privateKeyString.includes('BEGIN PRIVATE KEY')
          ? config.privateKeyString
          : `-----BEGIN PRIVATE KEY-----\n${config.privateKeyString}\n-----END PRIVATE KEY-----`

        const privateKeyObject = crypto.createPrivateKey({
          key: formattedKey,
          format: 'pem',
          ...(config.privateKeyPassphrase && { passphrase: config.privateKeyPassphrase }),
        })

        privateKey = privateKeyObject.export({
          format: 'pem',
          type: 'pkcs8',
        })
      } catch (err) {
        throw new Error('Invalid private key format')
      }

      connectionOptions = {
        account,
        username,
        privateKey: privateKey.toString(),
        database,
        warehouse,
        role,
        authenticator: 'SNOWFLAKE_JWT',
      }
    } else {
      throw new Error('Either token or privateKeyString must be provided')
    }

    this.log.info(
      {
        account,
        database,
        warehouse,
        username,
        maxConnections,
        minConnections,
      },
      'Initializing Snowflake connection pool',
    )
    this.pool = Snowflake.createPool(connectionOptions, {
      evictionRunIntervalMillis: 60000, // default = 0, off
      idleTimeoutMillis: 60000, // default = 30000
      max: maxConnections,
      min: minConnections,
    })
  }

  private async executeQuery(
    connection: Snowflake.Connection,
    query: string,
    binds?: any[],
  ): Promise<any[]> {
    return new Promise((resolve, reject) => {
      connection.execute({
        sqlText: query,
        binds: binds,
        complete: (err, stmt, rows) => {
          if (err) {
            reject(err)
          } else {
            resolve(rows)
          }
        },
      })
    })
  }

  public async run<T>(query: string, binds?: any[]): Promise<T[]> {
    return new Promise((resolve, reject) => {
      this.pool.use(async (connection) => {
        try {
          this.log.info(
            {
              query,
              binds,
              hasBinds: !!binds,
            },
            'Executing Snowflake query',
          )
          const results = await this.executeQuery(connection, query, binds)

          this.log.info(
            {
              query,
              resultCount: results.length,
            },
            'Successfully executed Snowflake query',
          )
          resolve(results)
        } catch (err) {
          reject(err)
        }
      })
    })
  }

  public async destroy(): Promise<void> {
    await this.pool.drain()
    await this.pool.clear()
  }

  public static fromEnv(extraConfig: any = {}) {
    const base = {
      account: process.env.CROWD_SNOWFLAKE_ACCOUNT,
      username: process.env.CROWD_SNOWFLAKE_USERNAME,
      database: process.env.CROWD_SNOWFLAKE_DATABASE,
      warehouse: process.env.CROWD_SNOWFLAKE_WAREHOUSE,
      role: process.env.CROWD_SNOWFLAKE_ROLE,
      maxConnections: 1,
      ...extraConfig,
    }

    if (process.env.CROWD_SNOWFLAKE_TOKEN) {
      return new SnowflakeClient({
        token: process.env.CROWD_SNOWFLAKE_TOKEN,
        ...base,
      })
    }

    return new SnowflakeClient({
      privateKeyString: process.env.CROWD_SNOWFLAKE_PRIVATE_KEY,
      ...base,
    })
  }
}
