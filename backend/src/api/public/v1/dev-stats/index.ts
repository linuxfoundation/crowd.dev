import { Router } from 'express'

import { createRateLimiter } from '@/api/apiRateLimiter'
import { requireScopes } from '@/api/public/middlewares/requireScopes'
import { SCOPES } from '@/security/scopes'

import { getAffiliations } from './getAffiliations'

const rateLimiter = createRateLimiter({ max: 60, windowMs: 60 * 1000 })

export function devStatsRouter(): Router {
  const router = Router()

  router.use(rateLimiter)

  router.post('/affiliations', requireScopes([SCOPES.READ_AFFILIATIONS]), getAffiliations)

  return router
}
