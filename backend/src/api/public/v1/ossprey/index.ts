import { Router } from 'express'

import { safeWrap } from '@/middlewares/errorMiddleware'

import { activityFeedHandler } from './activityFeed'
import { metricsHandler } from './metrics'
import { packageListHandler } from './packageList'
import { packageScatterHandler } from './packageScatter'

// TODO[deprecate]: superseded by /v1/akrites — ossprey endpoints are now at /v1/akrites/metrics,
// /v1/akrites/packages, /v1/akrites/packages/scatter, /v1/akrites/activity — remove once consumers have migrated
export function osspreyRouter(): Router {
  const router = Router()

  router.get('/metrics', safeWrap(metricsHandler))
  // /packages/scatter must be registered before /packages to avoid Express treating 'scatter' as a path param
  router.get('/packages/scatter', safeWrap(packageScatterHandler))
  router.get('/packages', safeWrap(packageListHandler))
  router.get('/activity', safeWrap(activityFeedHandler))

  return router
}
