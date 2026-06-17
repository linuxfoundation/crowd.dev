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
import { getPackageAdvisories } from '../packages/getPackageAdvisories'
import { getPackageHistory } from '../packages/getPackageHistory'
import { getPackagesMetrics } from '../packages/getPackagesMetrics'
import { assignStewardHandler } from '../stewardships/assignSteward'
import { escalateHandler } from '../stewardships/escalate'
import { openStewardship } from '../stewardships/openStewardship'
import { updateStatusHandler } from '../stewardships/updateStatus'

const rateLimiter = createRateLimiter({ max: 60, windowMs: 60 * 1000 })

export function akritesRouter(): Router {
  const router = Router()

  router.get('/metrics', safeWrap(metricsHandler))
  // /packages/scatter registered before router.use('/packages', ...) so Express evaluates this
  // explicit route first; without this ordering the sub-router would receive the request first
  // and call next() on no match, adding unnecessary overhead.
  router.get('/packages/scatter', rateLimiter, safeWrap(packageScatterHandler))
  router.get('/packages', rateLimiter, safeWrap(packageListHandler))
  router.get('/activity', safeWrap(activityFeedHandler))

  // --- packages ---
  router.post(
    /^\/packages:batch-stewardship\/?$/,
    rateLimiter,
    // TODO: restore once read:packages + read:stewardships are added to Auth0 staging tenant
    // requireScopes([SCOPES.READ_PACKAGES, SCOPES.READ_STEWARDSHIPS], 'any'),
    safeWrap(batchGetStewardship),
  )
  const packagesSubRouter = Router()
  packagesSubRouter.use(rateLimiter)
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
  packagesSubRouter.get(
    '/advisories',
    // TODO: restore once read:packages + read:stewardships are added to Auth0 staging tenant
    // requireScopes([SCOPES.READ_PACKAGES, SCOPES.READ_STEWARDSHIPS], 'any'),
    safeWrap(getPackageAdvisories),
  )
  packagesSubRouter.get(
    '/history',
    // TODO: restore once read:packages + read:stewardships are added to Auth0 staging tenant
    // requireScopes([SCOPES.READ_PACKAGES, SCOPES.READ_STEWARDSHIPS], 'any'),
    safeWrap(getPackageHistory),
  )
  router.use('/packages', packagesSubRouter)

  // --- stewardships ---
  const stewardshipsSubRouter = Router()
  stewardshipsSubRouter.use(rateLimiter)
  stewardshipsSubRouter.post(
    '/open',
    // TODO: restore once write:stewardships is added to Auth0 staging tenant
    // requireScopes([SCOPES.WRITE_STEWARDSHIPS]),
    safeWrap(openStewardship),
  )
  stewardshipsSubRouter.post(
    '/:id/assign',
    // TODO: restore once write:stewardships is added to Auth0 staging tenant
    // requireScopes([SCOPES.WRITE_STEWARDSHIPS]),
    safeWrap(assignStewardHandler),
  )
  stewardshipsSubRouter.post(
    '/:id/escalate',
    // TODO: restore once write:stewardships is added to Auth0 staging tenant
    // requireScopes([SCOPES.WRITE_STEWARDSHIPS]),
    safeWrap(escalateHandler),
  )
  stewardshipsSubRouter.patch(
    '/:id/status',
    // TODO: restore once write:stewardships is added to Auth0 staging tenant
    // requireScopes([SCOPES.WRITE_STEWARDSHIPS]),
    safeWrap(updateStatusHandler),
  )
  router.use('/stewardships', stewardshipsSubRouter)

  return router
}
