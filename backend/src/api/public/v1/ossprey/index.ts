import { Router } from 'express'

import { safeWrap } from '@/middlewares/errorMiddleware'

import { activityFeedHandler } from './activityFeed'
import { metricsHandler } from './metrics'
import { packageListHandler } from './packageList'
import { packageScatterHandler } from './packageScatter'

export function osspreyRouter(): Router {
  const router = Router()

  router.get('/metrics', safeWrap(metricsHandler))
  // /packages/scatter must be registered before /packages to avoid Express treating 'scatter' as a path param
  router.get('/packages/scatter', safeWrap(packageScatterHandler))
  router.get('/packages', safeWrap(packageListHandler))
  router.get('/activity', safeWrap(activityFeedHandler))

  return router
}
