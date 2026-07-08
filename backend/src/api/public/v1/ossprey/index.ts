import { Router } from 'express'

import { requireScopes } from '@/api/public/middlewares/requireScopes'
import { safeWrap } from '@/middlewares/errorMiddleware'
import { SCOPES } from '@/security/scopes'

import { activityFeedHandler } from './activityFeed'
import { metricsHandler } from './metrics'
import { packageListHandler } from './packageList'
import { packageScatterHandler } from './packageScatter'

// TODO[deprecate]: superseded by /v1/akrites — ossprey endpoints are now at /v1/akrites/metrics,
// /v1/akrites/packages, /v1/akrites/packages/scatter, /v1/akrites/activity — remove once consumers have migrated
export function osspreyRouter(): Router {
  const router = Router()

  router.get(
    '/metrics',
    requireScopes([SCOPES.READ_PACKAGES, SCOPES.READ_STEWARDSHIPS], 'all'),
    safeWrap(metricsHandler),
  )
  // /packages/scatter must be registered before /packages to avoid Express treating 'scatter' as a path param
  router.get(
    '/packages/scatter',
    requireScopes([SCOPES.READ_PACKAGES, SCOPES.READ_STEWARDSHIPS], 'all'),
    safeWrap(packageScatterHandler),
  )
  router.get(
    '/packages',
    requireScopes([SCOPES.READ_PACKAGES, SCOPES.READ_STEWARDSHIPS], 'all'),
    safeWrap(packageListHandler),
  )
  router.get(
    '/activity',
    requireScopes([SCOPES.READ_PACKAGES, SCOPES.READ_STEWARDSHIPS], 'all'),
    safeWrap(activityFeedHandler),
  )

  return router
}
