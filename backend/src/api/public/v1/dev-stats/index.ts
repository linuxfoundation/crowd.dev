import { Router } from 'express'

import { createRateLimiter } from '@/api/apiRateLimiter'
import { requireScopes } from '@/api/public/middlewares/requireScopes'
import { SCOPES } from '@/security/scopes'

const rateLimiter = createRateLimiter({ max: 60, windowMs: 60 * 1000 })

export function devStatsRouter(): Router {
  const router = Router()

  router.use(rateLimiter)

  router.post('/affiliations', requireScopes([SCOPES.READ_AFFILIATIONS]), (_req, res) => {
    res.json({ status: 'ok' })
  })

  return router
}
