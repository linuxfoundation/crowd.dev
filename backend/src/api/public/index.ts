import { Router } from 'express'

import { NotFoundError } from '@crowd/common'

import { errorHandler } from './middlewares/errorHandler'
import { v1Router } from './v1'

export function publicRouter(): Router {
  const router = Router()

  router.use('/v1', v1Router())

  router.use(() => {
    throw new NotFoundError()
  })

  router.use(errorHandler)

  return router
}
