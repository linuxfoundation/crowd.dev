import { Router } from 'express'

import { createRateLimiter } from '@/api/apiRateLimiter'
import { safeWrap } from '@/middlewares/errorMiddleware'
// TODO: restore once scopes are added to Auth0 staging tenant
// import { requireScopes } from '@/api/public/middlewares/requireScopes'
// import { SCOPES } from '@/security/scopes'

import { activityFeedHandler } from '../ossprey/activityFeed'
import { metricsHandler } from '../ossprey/metrics'
import { packageListHandler } from '../ossprey/packageList'
import { packageScatterHandler } from '../ossprey/packageScatter'
import { batchGetStewardship } from '../packages/batchGetStewardship'
import { getPackage } from '../packages/getPackage'
import { getPackagesMetrics } from '../packages/getPackagesMetrics'
import { assignStewardHandler } from '../stewardships/assignSteward'
import { escalateHandler } from '../stewardships/escalate'
import { openStewardship } from '../stewardships/openStewardship'
import { updateStatusHandler } from '../stewardships/updateStatus'

// Separate instances match the original per-router isolation: packages and stewardships each had
// their own createRateLimiter() call, giving independent 60 req/min buckets per IP.
const packagesRateLimiter = createRateLimiter({ max: 60, windowMs: 60 * 1000 })
const stewardshipsRateLimiter = createRateLimiter({ max: 60, windowMs: 60 * 1000 })

export function akritesRouter(): Router {
  const router = Router()

  router.get('/metrics', safeWrap(metricsHandler))
  // /packages/scatter registered before router.use('/packages', ...) so Express evaluates this
  // explicit route first; without this ordering the sub-router would receive the request first
  // and call next() on no match, adding unnecessary overhead.
  router.get('/packages/scatter', packagesRateLimiter, safeWrap(packageScatterHandler))
  router.get('/packages', packagesRateLimiter, safeWrap(packageListHandler))
  router.get('/activity', safeWrap(activityFeedHandler))

  // --- packages ---
  router.post(
    /^\/packages:batch-stewardship\/?$/,
    packagesRateLimiter,
    // TODO: restore once read:packages + read:stewardships are added to Auth0 staging tenant
    // requireScopes([SCOPES.READ_PACKAGES, SCOPES.READ_STEWARDSHIPS], 'any'),
    safeWrap(batchGetStewardship),
  )
  const packagesSubRouter = Router()
  packagesSubRouter.use(packagesRateLimiter)
  packagesSubRouter.get(
    '/metrics',
    // TODO: restore once read:packages + read:stewardships are added to Auth0 staging tenant
    // requireScopes([SCOPES.READ_PACKAGES, SCOPES.READ_STEWARDSHIPS], 'any'),
    safeWrap(getPackagesMetrics),
  )
  packagesSubRouter.get(
    '/detail',
    // TODO: restore once read:packages + read:stewardships are added to Auth0 staging tenant
    // requireScopes([SCOPES.READ_PACKAGES, SCOPES.READ_STEWARDSHIPS], 'any'),
    safeWrap(getPackage),
  )
  router.use('/packages', packagesSubRouter)

  // --- stewardships ---
  const stewardshipsSubRouter = Router()
  stewardshipsSubRouter.use(stewardshipsRateLimiter)
  stewardshipsSubRouter.post(
    '/',
    // TODO: restore once write:stewardships is added to Auth0 staging tenant
    // requireScopes([SCOPES.WRITE_STEWARDSHIPS]),
    safeWrap(openStewardship),
  )
  stewardshipsSubRouter.put(
    '/:id/steward',
    // TODO: restore once write:stewardships is added to Auth0 staging tenant
    // requireScopes([SCOPES.WRITE_STEWARDSHIPS]),
    safeWrap(assignStewardHandler),
  )
  stewardshipsSubRouter.put(
    '/:id/escalate',
    // TODO: restore once write:stewardships is added to Auth0 staging tenant
    // requireScopes([SCOPES.WRITE_STEWARDSHIPS]),
    safeWrap(escalateHandler),
  )
  stewardshipsSubRouter.put(
    '/:id/status',
    // TODO: restore once write:stewardships is added to Auth0 staging tenant
    // requireScopes([SCOPES.WRITE_STEWARDSHIPS]),
    safeWrap(updateStatusHandler),
  )
  router.use('/stewardships', stewardshipsSubRouter)

  return router
}
