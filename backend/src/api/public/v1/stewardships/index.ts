import { Router } from 'express'

import { createRateLimiter } from '@/api/apiRateLimiter'
// TODO: restore once write:stewardships is added to Auth0 staging tenant
// import { requireScopes } from '@/api/public/middlewares/requireScopes'
import { safeWrap } from '@/middlewares/errorMiddleware'

// import { SCOPES } from '@/security/scopes'
import { assignStewardHandler } from './assignSteward'
import { escalateHandler } from './escalate'
import { openStewardship } from './openStewardship'
import { updateStatusHandler } from './updateStatus'

const rateLimiter = createRateLimiter({ max: 60, windowMs: 60 * 1000 })

// TODO[deprecate]: superseded by /v1/akrites/stewardships — remove once consumers have migrated
export function stewardshipsRouter(): Router {
  const router = Router()

  router.use(rateLimiter)

  router.post(
    '/',
    // TODO: restore once write:stewardships is added to Auth0 staging tenant
    // requireScopes([SCOPES.WRITE_STEWARDSHIPS]),
    safeWrap(openStewardship),
  )

  router.put(
    '/:id/steward',
    // TODO: restore once write:stewardships is added to Auth0 staging tenant
    // requireScopes([SCOPES.WRITE_STEWARDSHIPS]),
    safeWrap(assignStewardHandler),
  )

  router.put(
    '/:id/escalate',
    // TODO: restore once write:stewardships is added to Auth0 staging tenant
    // requireScopes([SCOPES.WRITE_STEWARDSHIPS]),
    safeWrap(escalateHandler),
  )

  router.put(
    '/:id/status',
    // TODO: restore once write:stewardships is added to Auth0 staging tenant
    // requireScopes([SCOPES.WRITE_STEWARDSHIPS]),
    safeWrap(updateStatusHandler),
  )

  return router
}
