import {
  NativeConnection,
  Runtime,
  Worker as TemporalWorker,
  bundleWorkflowCode,
  makeTelemetryFilterString,
} from '@temporalio/worker'
import fs from 'fs'
import path from 'path'

import { Config, Service } from '@crowd/archetype-standard'
import { IS_DEV_ENV, IS_STAGING_ENV, IS_TEST_ENV } from '@crowd/common'
import { DbStore, getDbConnection } from '@crowd/database'
import { getServiceChildLogger } from '@crowd/logging'
import { OpenSearchService, getOpensearchClient } from '@crowd/opensearch'
import { IQueue, QueueFactory } from '@crowd/queue'
import { SlackChannel } from '@crowd/slack'
import { getDataConverter } from '@crowd/temporal'

import * as metricActivities from './activities'
import { ActivityMonitoringInterceptor } from './activities/activityInterceptor'
import { createTemporalLogger } from './logging'

// List all required environment variables, grouped per "component".
// They are in addition to the ones required by the "standard" archetype.
const envvars = {
  worker: [
    'CROWD_TEMPORAL_SERVER_URL',
    'CROWD_TEMPORAL_NAMESPACE',
    'CROWD_TEMPORAL_TASKQUEUE',
    'CROWD_TEMPORAL_ENCRYPTION_KEY_ID',
    'CROWD_TEMPORAL_ENCRYPTION_KEY',
  ],
  postgres: [
    'CROWD_DB_READ_HOST',
    'CROWD_DB_WRITE_HOST',
    'CROWD_DB_PORT',
    'CROWD_DB_USERNAME',
    'CROWD_DB_PASSWORD',
    'CROWD_DB_DATABASE',
  ],
  opensearch:
    IS_DEV_ENV || IS_TEST_ENV || IS_STAGING_ENV
      ? ['CROWD_OPENSEARCH_NODE']
      : ['CROWD_OPENSEARCH_USERNAME', 'CROWD_OPENSEARCH_PASSWORD', 'CROWD_OPENSEARCH_NODE'],
  queue: ['CROWD_KAFKA_BROKERS'],
}

/*
Options is used to configure the worker service.
*/
export interface Options {
  maxTaskQueueActivitiesPerSecond?: number
  maxConcurrentActivityTaskExecutions?: number
  alertChannel?: SlackChannel
  postgres?: {
    enabled: boolean
  }
  opensearch?: {
    enabled: boolean
  }
  queue?: {
    enabled: boolean
  }
}

/*
ServiceWorker holds all details and methods to run a worker services at crowd.dev.
*/
export class ServiceWorker extends Service {
  readonly options: Options

  protected _worker: TemporalWorker

  protected _postgresReader: DbStore
  protected _postgresWriter: DbStore

  protected _opensearchService: OpenSearchService

  protected _queueClient: IQueue

  constructor(config: Config, opts: Options) {
    super(config)

    this.options = opts
  }

  get postgres(): { reader: DbStore; writer: DbStore } | null {
    if (!this.options.postgres?.enabled) {
      return null
    }

    return {
      reader: this._postgresReader,
      writer: this._postgresWriter,
    }
  }

  get opensearch(): OpenSearchService {
    if (!this.options.opensearch?.enabled) {
      return null
    }

    return this._opensearchService
  }

  get queue(): IQueue {
    if (!this.options.queue?.enabled) {
      return null
    }

    return this._queueClient
  }

  // We first need to ensure a standard service can be initialized given the config
  // and environment variables.
  override async init(initWorker = true) {
    try {
      await super.init()
    } catch (err) {
      throw new Error(err)
    }

    // We can now init tasks specific to a consumer service. Before actually
    // starting the service, we need to ensure required environment variables
    // are set.
    const missing = []
    envvars.worker.forEach((envvar) => {
      if (!process.env[envvar]) {
        missing.push(envvar)
      }
    })

    // Only validate PostgreSQL-related environment variables if enabled.
    if (this.options.postgres?.enabled) {
      envvars.postgres.forEach((envvar) => {
        if (!process.env[envvar]) {
          missing.push(envvar)
        }
      })
    }

    // Only validate OpenSearch-related environment variables if enabled.
    if (this.options.opensearch?.enabled) {
      envvars.opensearch.forEach((envvar) => {
        if (!process.env[envvar]) {
          missing.push(envvar)
        }
      })
    }

    // Only validate queue related environment variables if enabled
    if (this.options.queue?.enabled) {
      envvars.queue.forEach((envvar) => {
        if (!process.env[envvar]) {
          missing.push(envvar)
        }
      })
    }

    // There's no point in continuing if a variable is missing.
    if (missing.length > 0) {
      throw new Error(`Missing environment variables: ${missing.join(', ')}`)
    }

    if (this.options.postgres?.enabled) {
      try {
        const dbConnection = await getDbConnection({
          host: process.env['CROWD_DB_READ_HOST'],
          port: Number(process.env['CROWD_DB_PORT']),
          user: process.env['CROWD_DB_USERNAME'],
          password: process.env['CROWD_DB_PASSWORD'],
          database: process.env['CROWD_DB_DATABASE'],
        })

        this._postgresReader = new DbStore(this.log, dbConnection)
      } catch (err) {
        throw new Error(err)
      }

      try {
        const dbConnection = await getDbConnection({
          host: process.env['CROWD_DB_WRITE_HOST'],
          port: Number(process.env['CROWD_DB_PORT']),
          user: process.env['CROWD_DB_USERNAME'],
          password: process.env['CROWD_DB_PASSWORD'],
          database: process.env['CROWD_DB_DATABASE'],
        })

        this._postgresWriter = new DbStore(this.log, dbConnection)
      } catch (err) {
        throw new Error(err)
      }
    }

    if (this.options.opensearch?.enabled) {
      const osClient = await getOpensearchClient({
        username: process.env['CROWD_OPENSEARCH_USERNAME'],
        password: process.env['CROWD_OPENSEARCH_PASSWORD'],
        node: process.env['CROWD_OPENSEARCH_NODE'],
      })
      try {
        this._opensearchService = new OpenSearchService(this.log, osClient)
      } catch (err) {
        throw new Error(err)
      }
    }

    if (this.options.queue?.enabled) {
      try {
        this._queueClient = QueueFactory.createQueueService({
          brokers: process.env['CROWD_KAFKA_BROKERS'],
          clientId: process.env['SERVICE'], //TODO:: make this configurable
          extra: process.env['CROWD_KAFKA_EXTRA'],
        })
      } catch (err) {
        throw new Error(err)
      }
    }

    if (initWorker) {
      try {
        Runtime.install({
          logger: createTemporalLogger(getServiceChildLogger('temporal-worker')),
          telemetryOptions: {
            logging: {
              forward: {},
              filter: makeTelemetryFilterString({ core: 'INFO' }),
            },
          },
        })

        const certificate = process.env['CROWD_TEMPORAL_CERTIFICATE']
        const privateKey = process.env['CROWD_TEMPORAL_PRIVATE_KEY']

        const address = process.env['CROWD_TEMPORAL_SERVER_URL']
        const taskQueue = process.env['CROWD_TEMPORAL_TASKQUEUE']
        const namespace = process.env['CROWD_TEMPORAL_NAMESPACE']

        this.log.info(
          {
            address,
            namespace,
            taskQueue,
            certificate: certificate ? 'yes' : 'no',
            privateKey: privateKey ? 'yes' : 'no',
          },
          'Connecting to Temporal server as a worker!',
        )

        const connection = await NativeConnection.connect({
          address,
          tls:
            certificate && privateKey
              ? {
                  clientCertPair: {
                    crt: Buffer.from(certificate, 'base64'),
                    key: Buffer.from(privateKey, 'base64'),
                  },
                }
              : undefined,
        })

        const workflowInterceptorModules = [path.join(__dirname, 'workflowInterceptors')]
        const serviceInterceptorsPath = path.resolve('./src/workflows/interceptors')
        if (fs.existsSync(serviceInterceptorsPath)) {
          workflowInterceptorModules.push(serviceInterceptorsPath)
        }

        const workflowBundle = await bundleWorkflowCode({
          workflowsPath: path.resolve('./src/workflows'),
          workflowInterceptorModules,
        })

        const dataConverter = await getDataConverter()

        this._worker = await TemporalWorker.create({
          connection: connection,
          identity: this.name,
          namespace,
          taskQueue,
          enableSDKTracing: true,
          showStackTraceSources: true,
          workflowBundle,
          activities: {
            ...metricActivities,
            ...require(path.resolve('./src/activities')),
          },
          interceptors: {
            activity: [
              (ctx) => {
                return {
                  inbound: new ActivityMonitoringInterceptor(ctx, this.options.alertChannel),
                }
              },
            ],
          },
          dataConverter: IS_DEV_ENV
            ? undefined
            : {
                ...dataConverter,
              },
          maxTaskQueueActivitiesPerSecond: this.options.maxTaskQueueActivitiesPerSecond,
          maxConcurrentActivityTaskExecutions: this.options.maxConcurrentActivityTaskExecutions,
        })
      } catch (err) {
        throw new Error(err)
      }
    }
  }

  // Actually start the Temporal worker.
  async start() {
    try {
      await this._worker.run()
    } catch (err) {
      throw new Error(err)
    }
  }

  // Stop allows to gracefully stop the service. Order for closing connections
  // matters. We need to stop the Temporal worker before closing other connections.
  protected override async stop() {
    if (this.options.opensearch?.enabled) {
      await this._opensearchService.client.close()
    }

    if (this.options.postgres?.enabled) {
      this._postgresWriter.dbInstance.end()
      this._postgresReader.dbInstance.end()
    }

    await super.stop()
  }
}
