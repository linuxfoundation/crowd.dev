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
import { ingestAkritesExternalContactDetail } from '../packages/ingestAkritesExternalContactDetail'
import { submitBlastRadiusJob } from '../packages/submitBlastRadiusJob'

const rateLimiter = createRateLimiter({ max: 60, windowMs: 60 * 1000 })

// Shared by every endpoint below that kicks off a Temporal workflow per request — those
// get their own, much stricter limiter than plain reads, configurable via env so it can
// be tuned without a redeploy.
function envTunableRateLimiter(envPrefix: string, defaultMax: number, defaultWindowMs: number) {
  const max = Number(process.env[`${envPrefix}_MAX`])
  const windowMs = Number(process.env[`${envPrefix}_WINDOW_MS`])
  return createRateLimiter({
    max: Number.isSafeInteger(max) && max > 0 ? max : defaultMax,
    windowMs: Number.isSafeInteger(windowMs) && windowMs > 0 ? windowMs : defaultWindowMs,
  })
}

// Blast-radius jobs default to 5 requests/hour.
const blastRadiusRateLimiter = envTunableRateLimiter(
  'AKRITES_BLAST_RADIUS_RATE_LIMIT',
  5,
  60 * 60 * 1000,
)

// /contacts/ingest starts a Temporal workflow and blocks for it (worst case ~95s per
// attempt cycle, plus unbounded time waiting for a free worker slot — see
// security-contacts/workflows.ts's singleActs config), vs. the read-only /contacts/detail
// endpoints, so it gets its own limiter. Defaults to 20 requests/hour.
const contactIngestRateLimiter = envTunableRateLimiter(
  'AKRITES_CONTACT_INGEST_RATE_LIMIT',
  20,
  60 * 60 * 1000,
)

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
  // requireScopes is applied per-route (not router-level) so each route can put its own
  // rate limiter *before* the scope check — failed-auth requests still count against that
  // route's quota — without forcing every route in this subrouter onto the same limiter
  // instance. /ingest gets its own dedicated contactIngestRateLimiter instead of sharing
  // the read endpoints' quota, matching the blast-radius jobs endpoint below.
  const contactsScopes = [SCOPES.READ_MAINTAINER_ROLES]
  const contactsSubRouter = Router()
  contactsSubRouter.get(
    '/detail',
    rateLimiter,
    requireScopes(contactsScopes),
    safeWrap(getAkritesExternalContactDetail),
  )
  contactsSubRouter.post(
    /^\/detail:batch\/?$/,
    rateLimiter,
    requireScopes(contactsScopes),
    safeWrap(getAkritesExternalContactDetailBatch),
  )
  // Sync, single-purl on-demand ingest — starts a Temporal workflow and blocks a while,
  // so it gets the dedicated contactIngestRateLimiter, not the shared rateLimiter above.
  contactsSubRouter.post(
    '/ingest',
    contactIngestRateLimiter,
    requireScopes(contactsScopes),
    safeWrap(ingestAkritesExternalContactDetail),
  )
  router.use('/contacts', contactsSubRouter)

  // TODO: the contract gates blast-radius behind a dedicated read:advisories scope
  // (same as advisories above — see the scope-naming note in the akrites-external
  // OpenAPI). Not issued by Auth0 yet, so reuse READ_PACKAGES for now.
  const blastRadiusSubRouter = Router()
  blastRadiusSubRouter.use(requireScopes([SCOPES.READ_PACKAGES]))
  blastRadiusSubRouter.post('/jobs', blastRadiusRateLimiter, safeWrap(submitBlastRadiusJob))
  blastRadiusSubRouter.get('/jobs/:analysisId', rateLimiter, safeWrap(getBlastRadiusJob))
  router.use('/blast-radius', blastRadiusSubRouter)

  return router
}
