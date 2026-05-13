import pg from 'pg'
import Sequelize, { DataTypes } from 'sequelize'

import { IS_CLOUD_ENV } from '@crowd/common'

/**
 * This module creates the Sequelize to the database and
 * exports all the models.
 */
import { getServiceChildLogger } from '@crowd/logging'

import { DB_CONFIG, SERVICE } from '../../conf'
import * as configTypes from '../../conf/configTypes'

const { highlight } = require('cli-highlight')

const log = getServiceChildLogger('Database')

pg.usingSequelize = true

interface Credentials {
  username: string
  password: string
}

function getCredentials(): Credentials {
  if (DB_CONFIG.username) {
    return {
      username: DB_CONFIG.username,
      password: DB_CONFIG.password,
    }
  }

  switch (SERVICE) {
    case configTypes.ServiceType.API:
      return {
        username: DB_CONFIG.apiUsername,
        password: DB_CONFIG.apiPassword,
      }
    case configTypes.ServiceType.JOB_GENERATOR:
      return {
        username: DB_CONFIG.jobGeneratorUsername,
        password: DB_CONFIG.jobGeneratorPassword,
      }
    default:
      throw new Error('Incorrectly configured database connection settings!')
  }
}

async function models(queryTimeoutMilliseconds: number, databaseHostnameOverride = null) {
  log.info('Initializing sequelize database connection!')
  const database = {} as any

  let readHost = SERVICE === configTypes.ServiceType.API ? DB_CONFIG.readHost : DB_CONFIG.writeHost
  let writeHost = DB_CONFIG.writeHost

  if (databaseHostnameOverride) {
    readHost = databaseHostnameOverride
    writeHost = databaseHostnameOverride
  }

  const credentials = getCredentials()

  const sequelize = new (Sequelize as any)(
    DB_CONFIG.database,
    credentials.username,
    credentials.password,
    {
      dialect: DB_CONFIG.dialect,
      dialectOptions: {
        application_name: SERVICE ? `${SERVICE}-seq` : 'unknown-app-seq',
        connectionTimeoutMillis: 15000,
        query_timeout: queryTimeoutMilliseconds,
        idle_in_transaction_session_timeout: 20000,
        ssl: IS_CLOUD_ENV ? { rejectUnauthorized: false } : false,
      },
      port: DB_CONFIG.port,
      replication: {
        read: [
          {
            host: readHost,
          },
        ],
        write: { host: writeHost },
      },
      pool: {
        max: SERVICE === configTypes.ServiceType.API ? 20 : 10,
        min: 1,
        acquire: 50000,
        idle: 10000,
      },
      logging: DB_CONFIG.logging
        ? (dbLog) =>
            log.info(
              highlight(dbLog, {
                language: 'sql',
                ignoreIllegals: true,
              }),
              'DB LOG',
            )
        : false,
    },
  )

  // if (profileQueries) {
  //   const oldQuery = sequelize.query
  //   sequelize.query = async (query, options) => {
  //     const { replacements } = options || {}
  //     const result = await logExecutionTimeV2(
  //       () => oldQuery.apply(sequelize, [query, options]),
  //       log,
  //       `DB Query:\n${query}\n${replacements ? `Params: ${JSON.stringify(replacements)}` : ''}`,
  //     )

  //     return result
  //   }
  // }

  const modelClasses = [
    require('./member').default,
    require('./memberIdentity').default,
    require('./file').default,
    require('./integration').default,
    require('./settings').default,
    require('./tenant').default,
    require('./tenantUser').default,
    require('./user').default,
    require('./eagleEyeContent').default,
    require('./eagleEyeAction').default,
    require('./organization').default,
    require('./memberAttributeSettings').default,
    require('./segment').default,
    require('./customView').default,
    require('./customViewOrder').default,
  ]

  for (const notInitmodel of modelClasses) {
    const model = notInitmodel(sequelize, DataTypes)
    database[model.name] = model
  }

  Object.keys(database).forEach((modelName) => {
    if (database[modelName].associate) {
      database[modelName].associate(database)
    }
  })

  database.sequelize = sequelize
  database.Sequelize = Sequelize

  await sequelize.authenticate()
  log.info('Sequelize database connection has been established successfully!')

  return database
}

export default models
