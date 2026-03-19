import { Router } from 'express'

import { AUTH0_CONFIG, DEV_STATS_CONFIG } from '../../conf'

import { errorHandler } from './middlewares/errorHandler'
import { oauth2Middleware } from './middlewares/oauth2Middleware'
import { staticApiKeyMiddleware } from './middlewares/staticApiKeyMiddleware'
import { v1Router } from './v1'
import { devStatsRouter } from './v1/dev-stats'

export function publicRouter(): Router {
  const router = Router()

  router.use('/v1/dev-stats', staticApiKeyMiddleware(), devStatsRouter())
  router.use('/v1', oauth2Middleware(AUTH0_CONFIG), v1Router())
  router.use(errorHandler)

  return router
}
