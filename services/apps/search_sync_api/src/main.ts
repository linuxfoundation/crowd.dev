import cors from 'cors'
import express from 'express'

import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { getServiceLogger } from '@crowd/logging'
import { InitService, OpenSearchService, getOpensearchClient } from '@crowd/opensearch'
import { getRedisClient } from '@crowd/redis'
import { telemetryExpressMiddleware } from '@crowd/telemetry'

import { DB_CONFIG, OPENSEARCH_CONFIG, REDIS_CONFIG, SEARCH_SYNC_API_CONFIG } from './conf'
import { ApiRequest } from './middleware'
import { databaseMiddleware } from './middleware/database'
import { errorMiddleware } from './middleware/error'
import { loggingMiddleware } from './middleware/logging'
import { opensearchMiddleware } from './middleware/opensearch'
import { redisMiddleware } from './middleware/redis'
import memberRoutes from './routes/member'
import organizationRoutes from './routes/organization'

const log = getServiceLogger()
const config = SEARCH_SYNC_API_CONFIG()

setImmediate(async () => {
  const app = express()
  const redis = await getRedisClient(REDIS_CONFIG(), true)
  const osClient = await getOpensearchClient(OPENSEARCH_CONFIG())
  const opensearch = new OpenSearchService(log, osClient)
  const pgConnection = await getDbConnection(DB_CONFIG(), 5, 5000)

  app.use(telemetryExpressMiddleware('search_sync_api.request.duration'))
  app.use(cors({ origin: true }))
  app.use(express.json({ limit: '5mb' }))
  app.use(express.urlencoded({ extended: true, limit: '5mb' }))
  app.use(loggingMiddleware(log))
  app.use(redisMiddleware(redis))
  app.use(databaseMiddleware(pgConnection))
  app.use(opensearchMiddleware(opensearch))

  // init opensearch service
  const initService = new InitService(opensearch)
  await initService.initialize()

  // add routes
  app.use(memberRoutes)
  app.use(organizationRoutes)

  app.use('/health', async (req: ApiRequest, res) => {
    try {
      const [opensearchCheck, redisCheck, dbCheck] = await Promise.all([
        // ping opensearch
        osClient.ping().then((res) => res.body),
        // ping redis,
        redis.ping().then((res) => res === 'PONG'),
        // ping database
        req.pgStore
          .connection()
          .any('select 1')
          .then((rows) => rows.length === 1),
      ])

      if (opensearchCheck && redisCheck && dbCheck) {
        res.sendStatus(200)
      } else {
        res.status(500).json({
          opensearch: opensearchCheck,
          redis: redisCheck,
          database: dbCheck,
        })
      }
    } catch (err) {
      res.status(500).json({ error: err })
    }
  })

  app.use(errorMiddleware())

  app.listen(config.port, () => {
    log.info(`Search Sync API listening on port ${config.port}!`)
  })
})
