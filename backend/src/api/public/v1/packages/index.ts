import { Router } from 'express'

import { createRateLimiter } from '@/api/apiRateLimiter'
import { requireScopes } from '@/api/public/middlewares/requireScopes'
import { safeWrap } from '@/middlewares/errorMiddleware'
import { SCOPES } from '@/security/scopes'

import { getPackage } from './getPackage'
import { getPackagesMetrics } from './getPackagesMetrics'
import { listPackages } from './listPackages'

const rateLimiter = createRateLimiter({ max: 60, windowMs: 60 * 1000 })

export function packagesRouter(): Router {
  const router = Router()

  router.use(rateLimiter)

  router.get(
    '/',
    // TODO: restore once read:packages + read:stewardships are added to Auth0 staging tenant
    // requireScopes([SCOPES.READ_PACKAGES, SCOPES.READ_STEWARDSHIPS], 'any'),
    safeWrap(listPackages),
  )

  router.get(
    '/metrics',
    // TODO: restore once read:packages + read:stewardships are added to Auth0 staging tenant
    // requireScopes([SCOPES.READ_PACKAGES, SCOPES.READ_STEWARDSHIPS], 'any'),
    safeWrap(getPackagesMetrics),
  )

  router.get(
    '/:purl',
    // TODO: restore once read:packages + read:stewardships are added to Auth0 staging tenant
    // requireScopes([SCOPES.READ_PACKAGES, SCOPES.READ_STEWARDSHIPS], 'any'),
    safeWrap(getPackage),
  )

  return router
}
