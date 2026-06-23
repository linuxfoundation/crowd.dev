import { Router } from 'express'

import { createRateLimiter } from '@/api/apiRateLimiter'
import { requireScopes } from '@/api/public/middlewares/requireScopes'
import { safeWrap } from '@/middlewares/errorMiddleware'
import { SCOPES } from '@/security/scopes'

import { assignStewardHandler } from './assignSteward'
import { escalateHandler } from './escalate'
import { openStewardship } from './openStewardship'
import { updateStatusHandler } from './updateStatus'

const rateLimiter = createRateLimiter({ max: 60, windowMs: 60 * 1000 })

// TODO[deprecate]: superseded by /v1/akrites/stewardships — remove once consumers have migrated
export function stewardshipsRouter(): Router {
  const router = Router()

  router.use(rateLimiter)

  router.post('/', requireScopes([SCOPES.WRITE_STEWARDSHIPS]), safeWrap(openStewardship))

  router.put(
    '/:id/steward',
    requireScopes([SCOPES.WRITE_STEWARDSHIPS]),
    safeWrap(assignStewardHandler),
  )

  router.put('/:id/escalate', requireScopes([SCOPES.WRITE_STEWARDSHIPS]), safeWrap(escalateHandler))

  router.put(
    '/:id/status',
    requireScopes([SCOPES.WRITE_STEWARDSHIPS]),
    safeWrap(updateStatusHandler),
  )

  return router
}
