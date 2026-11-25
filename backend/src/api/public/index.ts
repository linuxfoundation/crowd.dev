import { Router } from 'express'

import type { RedisClient } from '@crowd/redis'

import { errorHandler } from './middlewares/errorHandler'
import { v1Router } from './v1'

export function publicRouter({ redis }: { redis: RedisClient }): Router {
  const router = Router()

  router.use('/v1', v1Router({ redis }))
  router.use(errorHandler)

  return router
}
