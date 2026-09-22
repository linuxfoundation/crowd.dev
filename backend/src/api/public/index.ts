import { Router } from 'express'

import { errorHandler } from './middlewares/errorHandler'
import { v1Router } from './v1'

export function publicRouter(): Router {
  const router = Router()

  router.use('/v1', v1Router())
  router.use(errorHandler)

  return router
}
