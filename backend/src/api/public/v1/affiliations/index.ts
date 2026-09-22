import { Router } from 'express'

import { createRateLimiter } from '@/api/apiRateLimiter'
import { requireScopes } from '@/api/public/middlewares/requireScopes'
import { safeWrap } from '@/middlewares/errorMiddleware'
import { SCOPES } from '@/security/scopes'

import { getAffiliationByHandle } from './getAffiliationByHandle'
import { getAffiliations } from './getAffiliations'

const rateLimiter = createRateLimiter({ max: 60, windowMs: 60 * 1000 })

export function memberOrganizationAffiliationsRouter(): Router {
  const router = Router()

  router.use(rateLimiter)

  router.post('/', requireScopes([SCOPES.READ_AFFILIATIONS]), safeWrap(getAffiliations))
  router.get(
    '/:githubHandle',
    requireScopes([SCOPES.READ_AFFILIATIONS]),
    safeWrap(getAffiliationByHandle),
  )

  return router
}
