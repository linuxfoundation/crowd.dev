import { Router } from 'express'

import { createRateLimiter } from '@/api/apiRateLimiter'
import { safeWrap } from '@/middlewares/errorMiddleware'

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

const rateLimiter = createRateLimiter({ max: 60, windowMs: 60 * 1000 })

export function akritesRouter(): Router {
  const router = Router()

  router.get('/metrics', safeWrap(metricsHandler))
  // /packages/scatter must be registered before /packages to avoid Express treating 'scatter' as a path param
  router.get('/packages/scatter', safeWrap(packageScatterHandler))
  router.get('/packages', safeWrap(packageListHandler))
  router.get('/activity', safeWrap(activityFeedHandler))

  // --- packages ---
  router.post(/^\/packages:batch-stewardship\/?$/, rateLimiter, safeWrap(batchGetStewardship))
  const packagesSubRouter = Router()
  packagesSubRouter.use(rateLimiter)
  packagesSubRouter.get('/metrics', safeWrap(getPackagesMetrics))
  packagesSubRouter.get('/detail', safeWrap(getPackage))
  router.use('/packages', packagesSubRouter)

  // --- stewardships ---
  const stewardshipsSubRouter = Router()
  stewardshipsSubRouter.use(rateLimiter)
  stewardshipsSubRouter.post('/', safeWrap(openStewardship))
  stewardshipsSubRouter.put('/:id/steward', safeWrap(assignStewardHandler))
  stewardshipsSubRouter.put('/:id/escalate', safeWrap(escalateHandler))
  stewardshipsSubRouter.put('/:id/status', safeWrap(updateStatusHandler))
  router.use('/stewardships', stewardshipsSubRouter)

  return router
}
