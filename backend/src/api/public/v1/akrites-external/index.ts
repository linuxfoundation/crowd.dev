import { Router } from 'express'

import { createRateLimiter } from '@/api/apiRateLimiter'
import { requireScopes } from '@/api/public/middlewares/requireScopes'
import { safeWrap } from '@/middlewares/errorMiddleware'
import { SCOPES } from '@/security/scopes'

import { getAkritesExternalAdvisoryDetail } from '../packages/getAkritesExternalAdvisoryDetail'
import { getAkritesExternalAdvisoryDetailBatch } from '../packages/getAkritesExternalAdvisoryDetailBatch'
import { getAkritesExternalContactDetail } from '../packages/getAkritesExternalContactDetail'
import { getAkritesExternalContactDetailBatch } from '../packages/getAkritesExternalContactDetailBatch'
import { getAkritesExternalPackageDetail } from '../packages/getAkritesExternalPackageDetail'
import { getAkritesExternalPackageDetailBatch } from '../packages/getAkritesExternalPackageDetailBatch'
import { getBlastRadiusJob } from '../packages/getBlastRadiusJob'
import { getBlastRadiusJobBatch } from '../packages/getBlastRadiusJobBatch'
import { submitBlastRadiusJob } from '../packages/submitBlastRadiusJob'
import { submitBlastRadiusJobBatch } from '../packages/submitBlastRadiusJobBatch'

const rateLimiter = createRateLimiter({ max: 60, windowMs: 60 * 1000 })

// Blast-radius jobs kick off a Temporal workflow per request, so they get their own,
// much stricter limiter — configurable via env so it can be tuned without a redeploy.
// Defaults to 5 requests/hour.
const blastRadiusRateLimitMax = Number(process.env.AKRITES_BLAST_RADIUS_RATE_LIMIT_MAX)
const blastRadiusRateLimitWindowMs = Number(process.env.AKRITES_BLAST_RADIUS_RATE_LIMIT_WINDOW_MS)

const blastRadiusRateLimiter = createRateLimiter({
  max:
    Number.isSafeInteger(blastRadiusRateLimitMax) && blastRadiusRateLimitMax > 0
      ? blastRadiusRateLimitMax
      : 5,
  windowMs:
    Number.isSafeInteger(blastRadiusRateLimitWindowMs) && blastRadiusRateLimitWindowMs > 0
      ? blastRadiusRateLimitWindowMs
      : 60 * 60 * 1000,
})

export function akritesExternalRouter(): Router {
  const router = Router()

  // TODO: swap for a dedicated cdp:packages:read scope once Akrites gets its own
  // Auth0 M2M scopes (per the akrites-external draft contract) — reusing the
  // internal CDP UI scopes for now since that's what's actually issued today.
  const packagesSubRouter = Router()
  packagesSubRouter.use(rateLimiter)
  packagesSubRouter.use(requireScopes([SCOPES.READ_PACKAGES, SCOPES.READ_STEWARDSHIPS], 'all'))
  packagesSubRouter.get('/detail', safeWrap(getAkritesExternalPackageDetail))
  packagesSubRouter.post(/^\/detail:batch\/?$/, safeWrap(getAkritesExternalPackageDetailBatch))
  router.use('/packages', packagesSubRouter)

  // TODO: the contract gates advisories behind a dedicated read:advisories scope
  // (see the scope-naming note in the akrites-external OpenAPI). That scope isn't
  // issued by Auth0 yet, so reuse READ_PACKAGES for now — advisories are package
  // security data and, unlike the packages endpoints above, need no stewardship read.
  const advisoriesSubRouter = Router()
  advisoriesSubRouter.use(rateLimiter)
  advisoriesSubRouter.use(requireScopes([SCOPES.READ_PACKAGES]))
  advisoriesSubRouter.get('/detail', safeWrap(getAkritesExternalAdvisoryDetail))
  advisoriesSubRouter.post(/^\/detail:batch\/?$/, safeWrap(getAkritesExternalAdvisoryDetailBatch))
  router.use('/advisories', advisoriesSubRouter)

  // Security contacts expose contact PII (e.g. reporter emails), so the contract gates
  // them behind a dedicated cdp:maintainers:read scope and explicitly forbids reaching
  // them via the packages scope. That scope isn't issued by Auth0 yet, so reuse the
  // closest issued one — READ_MAINTAINER_ROLES (maintainer data) — NOT READ_PACKAGES.
  // TODO: swap for cdp:maintainers:read once issued.
  const contactsSubRouter = Router()
  contactsSubRouter.use(rateLimiter)
  contactsSubRouter.use(requireScopes([SCOPES.READ_MAINTAINER_ROLES]))
  contactsSubRouter.get('/detail', safeWrap(getAkritesExternalContactDetail))
  contactsSubRouter.post(/^\/detail:batch\/?$/, safeWrap(getAkritesExternalContactDetailBatch))
  router.use('/contacts', contactsSubRouter)

  // TODO: the contract gates blast-radius behind a dedicated read:advisories scope
  // (same as advisories above — see the scope-naming note in the akrites-external
  // OpenAPI). Not issued by Auth0 yet, so reuse READ_PACKAGES for now.
  const blastRadiusSubRouter = Router()
  blastRadiusSubRouter.use(requireScopes([SCOPES.READ_PACKAGES]))
  blastRadiusSubRouter.post('/jobs', blastRadiusRateLimiter, safeWrap(submitBlastRadiusJob))
  // Bulk submit multiplies Temporal workflow starts per request (up to
  // MAX_BLAST_RADIUS_JOBS_PER_BATCH), so it sits behind the same strict
  // blastRadiusRateLimiter as the single-job route, not the regular one.
  blastRadiusSubRouter.post(
    /^\/jobs:batch\/?$/,
    blastRadiusRateLimiter,
    safeWrap(submitBlastRadiusJobBatch),
  )
  blastRadiusSubRouter.get('/jobs/:analysisId', rateLimiter, safeWrap(getBlastRadiusJob))
  // Bulk poll is read-only, same cost profile as the other batch endpoints, so
  // it uses the regular rateLimiter.
  blastRadiusSubRouter.post(
    /^\/jobs:batch\/poll\/?$/,
    rateLimiter,
    safeWrap(getBlastRadiusJobBatch),
  )
  router.use('/blast-radius', blastRadiusSubRouter)

  return router
}
