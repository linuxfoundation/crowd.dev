import { Router } from 'express'

import { createRateLimiter } from '@/api/apiRateLimiter'
import { requireScopes } from '@/api/public/middlewares/requireScopes'
import { safeWrap } from '@/middlewares/errorMiddleware'
import { SCOPES } from '@/security/scopes'

import { getAkritesExternalAdvisoryDetail } from '../packages/getAkritesExternalAdvisoryDetail'
import { getAkritesExternalAdvisoryDetailBatch } from '../packages/getAkritesExternalAdvisoryDetailBatch'
import { getAkritesExternalBlastRadiusJob } from '../packages/getAkritesExternalBlastRadiusPoll'
import { submitAkritesExternalBlastRadiusJob } from '../packages/getAkritesExternalBlastRadiusSubmit'
import { getAkritesExternalContactDetail } from '../packages/getAkritesExternalContactDetail'
import { getAkritesExternalContactDetailBatch } from '../packages/getAkritesExternalContactDetailBatch'
import { getAkritesExternalPackageDetail } from '../packages/getAkritesExternalPackageDetail'
import { getAkritesExternalPackageDetailBatch } from '../packages/getAkritesExternalPackageDetailBatch'

const rateLimiter = createRateLimiter({ max: 60, windowMs: 60 * 1000 })

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

  // Same read:advisories scope as Advisories above (see the contract's Blast Radius
  // tag). The reachability pipeline isn't built yet, so every request 501s — see
  // getAkritesExternalBlastRadiusSubmit/Poll — but the route/scope wiring is real.
  const blastRadiusSubRouter = Router()
  blastRadiusSubRouter.use(rateLimiter)
  blastRadiusSubRouter.use(requireScopes([SCOPES.READ_PACKAGES]))
  blastRadiusSubRouter.post('/jobs', safeWrap(submitAkritesExternalBlastRadiusJob))
  blastRadiusSubRouter.get('/jobs/:analysisId', safeWrap(getAkritesExternalBlastRadiusJob))
  router.use('/blast-radius', blastRadiusSubRouter)

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

  return router
}
