import { Router } from 'express'

import { createRateLimiter } from '@/api/apiRateLimiter'
// TODO: restore once read:packages + read:stewardships are added to Auth0 staging tenant
// import { requireScopes } from '@/api/public/middlewares/requireScopes'
import { safeWrap } from '@/middlewares/errorMiddleware'

// import { SCOPES } from '@/security/scopes'
import { getPackage } from './getPackage'
import { getPackagesMetrics } from './getPackagesMetrics'
import { listPackages } from './listPackages'

const rateLimiter = createRateLimiter({ max: 60, windowMs: 60 * 1000 })

// TODO[deprecate]: /packages/metrics and /packages/detail are superseded by /v1/akrites/packages/metrics
// and /v1/akrites/packages/detail — remove once consumers have migrated.
// NOTE: GET /packages (listPackages) is intentionally NOT replicated in /v1/akrites because it has a
// different response shape from GET /v1/akrites/packages (ossprey packageListHandler). Before removing,
// verify no consumer calls GET /v1/packages — if unused, delete listPackages and this route entirely.
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
    '/detail',
    // TODO: restore once read:packages + read:stewardships are added to Auth0 staging tenant
    // requireScopes([SCOPES.READ_PACKAGES, SCOPES.READ_STEWARDSHIPS], 'any'),
    safeWrap(getPackage),
  )

  return router
}
