import bodyParser from 'body-parser'
import bunyanMiddleware from 'bunyan-middleware'
import cors from 'cors'
import express from 'express'
import helmet from 'helmet'
import * as http from 'http'
import os from 'os'
import { QueryTypes } from 'sequelize'

import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { getServiceLogger } from '@crowd/logging'
import { getOpensearchClient } from '@crowd/opensearch'
import { RedisPubSubReceiver, getRedisClient, getRedisPubSubPair } from '@crowd/redis'
import { telemetryExpressMiddleware } from '@crowd/telemetry'
import { Client as TemporalClient, getTemporalClient } from '@crowd/temporal'
import { ApiWebsocketMessage } from '@crowd/types'

import SequelizeRepository from '@/database/repositories/sequelizeRepository'
import { productDatabaseMiddleware } from '@/middlewares/productDbMiddleware'

import { OPENSEARCH_CONFIG, PRODUCT_DB_CONFIG, REDIS_CONFIG, TEMPORAL_CONFIG } from '../conf'
import { authMiddleware } from '../middlewares/authMiddleware'
import { databaseMiddleware } from '../middlewares/databaseMiddleware'
import { errorMiddleware } from '../middlewares/errorMiddleware'
import { languageMiddleware } from '../middlewares/languageMiddleware'
import { opensearchMiddleware } from '../middlewares/opensearchMiddleware'
import { passportStrategyMiddleware } from '../middlewares/passportStrategyMiddleware'
import { redisMiddleware } from '../middlewares/redisMiddleware'
import { responseHandlerMiddleware } from '../middlewares/responseHandlerMiddleware'
import { segmentMiddleware } from '../middlewares/segmentMiddleware'
import { tenantMiddleware } from '../middlewares/tenantMiddleware'

import { createRateLimiter } from './apiRateLimiter'
import authSocial from './auth/authSocial'
import { publicRouter } from './public'
import WebSockets from './websockets'

const serviceLogger = getServiceLogger()

const app = express()

const server = http.createServer(app)

setImmediate(async () => {
  const redis = await getRedisClient(REDIS_CONFIG, true)

  const opensearch = await getOpensearchClient(OPENSEARCH_CONFIG)

  const redisPubSubPair = await getRedisPubSubPair(REDIS_CONFIG)
  const userNamespace = await WebSockets.initialize(server)

  const pubSubReceiver = new RedisPubSubReceiver(
    'api-pubsub',
    redisPubSubPair.subClient,
    (err) => {
      serviceLogger.error(err, 'Error while listening to Redis Pub/Sub api-ws channel!')
      process.exit(1)
    },
    serviceLogger,
  )

  pubSubReceiver.subscribe('user', async (message) => {
    const data = message as ApiWebsocketMessage

    if (data.tenantId) {
      await userNamespace.emitForTenant(data.tenantId, data.event, data.data)
    } else if (data.userId) {
      userNamespace.emitToUserRoom(data.userId, data.event, data.data)
    } else {
      serviceLogger.error({ type: data.type }, 'Received invalid websocket message!')
    }
  })

  app.use(telemetryExpressMiddleware('api.request.duration'))

  // Enables CORS
  app.use(cors({ origin: true }))

  // Logging middleware
  app.use(
    bunyanMiddleware({
      headerName: 'x-request-id',
      propertyName: 'requestId',
      logName: `requestId`,
      logger: serviceLogger,
      level: 'trace',
    }),
  )

  app.use((req, res, next) => {
    // @ts-ignore
    req.profileSql = req.headers['x-profile-sql'] === 'true'
    next()
  })

  app.use((req, res, next) => {
    res.setHeader('X-Hostname', os.hostname())
    next()
  })

  app.use((req, res, next) => {
    // this middleware fixes the issue with logging and datadog
    // explained in detail here: https://github.com/CrowdDotDev/crowd.dev/pull/2144
    // in short: the hostname field in logs breaks how datadog assigns k8s cluster info
    if (req.log.fields.hostname) {
      delete req.log.fields.hostname
    }

    next()
  })

  // Initializes and adds the database middleware.
  app.use(databaseMiddleware)

  // Bind redis to request
  app.use(redisMiddleware(redis))

  // bind opensearch
  app.use(opensearchMiddleware(opensearch))

  // temp check for production
  if (TEMPORAL_CONFIG.serverUrl) {
    // Bind temporal to request
    const temporal = await getTemporalClient(TEMPORAL_CONFIG)
    app.use((req: any, res, next) => {
      req.temporal = temporal
      next()
    })
  }

  // Enables Helmet, a set of tools to
  // increase security.
  app.use(helmet())

  const defaultRateLimiter = createRateLimiter({
    max: 200,
    windowMs: 60 * 1000,
  })

  app.use(defaultRateLimiter)

  app.use(
    bodyParser.json({
      limit: '5mb',
    }),
  )

  app.use(bodyParser.urlencoded({ limit: '5mb', extended: true }))

  app.use((req, res, next) => {
    // @ts-ignore
    req.userData = {
      ip: req.ip,
      userAgent: req.headers ? req.headers['user-agent'] : null,
    }

    next()
  })

  // Public API uses its own OAuth2 auth and error flow
  // Must be mounted before internal endpoints.
  app.use('/', publicRouter())

  // initialize passport strategies
  app.use(passportStrategyMiddleware)

  // Sets the current language of the request
  app.use(languageMiddleware)

  // adds our ApiResponseHandler instance to the req object as responseHandler
  app.use(responseHandlerMiddleware)

  // Configures the authentication middleware
  // to set the currentUser to the requests
  app.use(authMiddleware)

  app.use('/health', async (req: any, res) => {
    try {
      const seq = SequelizeRepository.getSequelize(req)

      const [osPingRes, redisPingRes, dbPingRes, temporalPingRes] = await Promise.all([
        // ping opensearch
        opensearch.ping().then((res) => res.body),
        // ping redis,
        redis.ping().then((res) => res === 'PONG'),
        // ping database
        seq.query('select 1', { type: QueryTypes.SELECT }).then((rows) => rows.length === 1),
        // ping temporal
        req.temporal
          ? (req.temporal as TemporalClient).workflowService.getSystemInfo({}).then(() => true)
          : Promise.resolve(true),
      ])

      if (osPingRes && redisPingRes && dbPingRes && temporalPingRes) {
        res.sendStatus(200)
      } else {
        res.status(500).json({
          opensearch: osPingRes,
          redis: redisPingRes,
          database: dbPingRes,
          temporal: temporalPingRes,
        })
      }
    } catch (err) {
      res.status(500).json({ error: err.message, stack: err.stack })
    }
  })

  // Configure the Entity routes
  const routes = express.Router()

  // Enable Passport for Social Sign-in
  authSocial(app, routes)

  // Enable product db only if it's configured
  if (PRODUCT_DB_CONFIG) {
    const productDbClient = await getDbConnection(PRODUCT_DB_CONFIG)
    app.use(productDatabaseMiddleware(productDbClient))
    require('./product').default(routes)
  }

  require('./auth').default(routes)

  app.use(tenantMiddleware)
  app.use(segmentMiddleware)

  require('./auditLog').default(routes)
  require('./merge-suggestions').default(routes)
  require('./user').default(routes)
  require('./settings').default(routes)
  require('./member').default(routes)
  require('./activity').default(routes)
  require('./integration').default(routes)
  require('./eagleEyeContent').default(routes)
  require('./organization').default(routes)
  require('./slack').default(routes)
  require('./segment').default(routes)
  require('./systemStatus').default(routes)
  require('./customViews').default(routes)
  require('./dashboard').default(routes)
  require('./mergeAction').default(routes)
  require('./dataQuality').default(routes)
  require('./collections').default(routes)
  require('./categories').default(routes)

  await require('./nango').default(routes)

  app.use('/', routes)

  app.use(errorMiddleware)
})

export default server
