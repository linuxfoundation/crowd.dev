import { Router } from 'express'

import { createRateLimiter } from '@/api/apiRateLimiter'
import { requireScopes } from '@/api/public/middlewares/requireScopes'
import { safeWrap } from '@/middlewares/errorMiddleware'
import { SCOPES } from '@/security/scopes'

import { getPackage } from './getPackage'

const rateLimiter = createRateLimiter({ max: 60, windowMs: 60 * 1000 })

export function packagesRouter(): Router {
  const router = Router()

  router.use(rateLimiter)

  router.get(
    '/:purl',
    requireScopes([SCOPES.READ_PACKAGES, SCOPES.READ_STEWARDSHIPS]),
    safeWrap(getPackage),
  )

  return router
}
