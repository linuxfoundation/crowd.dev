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
import { getAkritesExternalProjectProfiling } from '../packages/getAkritesExternalProjectProfiling'
import { getBlastRadiusJob } from '../packages/getBlastRadiusJob'
import { getBlastRadiusJobBatch } from '../packages/getBlastRadiusJobBatch'
import { ingestAkritesExternalContactDetail } from '../packages/ingestAkritesExternalContactDetail'
import { submitBlastRadiusJob } from '../packages/submitBlastRadiusJob'
import { submitBlastRadiusJobBatch } from '../packages/submitBlastRadiusJobBatch'

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

// Blast-radius jobs default to 50 requests/hour.
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

  // Any one of the dedicated Akrites scope or the old Self Serve scopes works for now —
  // drop READ_PACKAGES/READ_STEWARDSHIPS once Akrites cuts over.
  const packagesSubRouter = Router()
  packagesSubRouter.use(rateLimiter)
  packagesSubRouter.use(
    requireScopes(
      [SCOPES.READ_AKRITES_PACKAGES, SCOPES.READ_PACKAGES, SCOPES.READ_STEWARDSHIPS],
      'any',
    ),
  )
  packagesSubRouter.get('/detail', safeWrap(getAkritesExternalPackageDetail))
  packagesSubRouter.post(/^\/detail:batch\/?$/, safeWrap(getAkritesExternalPackageDetailBatch))
  router.use('/packages', packagesSubRouter)

  // Reporting protocol is package/repo security metadata (no contact PII), so it rides
  // the same scope set as /packages, not the maintainer scope.
  router.get(
    '/project-profiling',
    rateLimiter,
    requireScopes(
      [SCOPES.READ_AKRITES_PACKAGES, SCOPES.READ_PACKAGES, SCOPES.READ_STEWARDSHIPS],
      'any',
    ),
    safeWrap(getAkritesExternalProjectProfiling),
  )

  // Dedicated read:akrites-advisories, or Self Serve's read:packages as a
  // fallback until Akrites cuts over — drop it then.
  const advisoriesScopes = [SCOPES.READ_PACKAGES, SCOPES.READ_AKRITES_ADVISORIES]
  const advisoriesSubRouter = Router()
  advisoriesSubRouter.use(rateLimiter)
  advisoriesSubRouter.use(requireScopes(advisoriesScopes, 'any'))
  advisoriesSubRouter.get('/detail', safeWrap(getAkritesExternalAdvisoryDetail))
  advisoriesSubRouter.post(/^\/detail:batch\/?$/, safeWrap(getAkritesExternalAdvisoryDetailBatch))
  router.use('/advisories', advisoriesSubRouter)

  // Contact PII stays behind a dedicated scope, never the packages scope: dedicated
  // read:akrites-maintainers, or Self Serve's read:maintainer-roles as a fallback.
  //
  // requireScopes is applied per-route (not router-level) so each route can put its own
  // rate limiter *before* the scope check — failed-auth requests still count against that
  // route's quota — without forcing every route in this subrouter onto the same limiter
  // instance. /ingest gets its own dedicated contactIngestRateLimiter instead of sharing
  // the read endpoints' quota, matching the blast-radius jobs endpoint below.
  const contactsScopes = [SCOPES.READ_MAINTAINER_ROLES, SCOPES.READ_AKRITES_MAINTAINERS]
  const contactsSubRouter = Router()
  contactsSubRouter.get(
    '/detail',
    rateLimiter,
    requireScopes(contactsScopes, 'any'),
    safeWrap(getAkritesExternalContactDetail),
  )
  contactsSubRouter.post(
    /^\/detail:batch\/?$/,
    rateLimiter,
    requireScopes(contactsScopes, 'any'),
    safeWrap(getAkritesExternalContactDetailBatch),
  )
  // Sync, single-purl on-demand ingest — starts a Temporal workflow and blocks a while,
  // so it gets the dedicated contactIngestRateLimiter, not the shared rateLimiter above.
  contactsSubRouter.post(
    '/ingest',
    contactIngestRateLimiter,
    requireScopes(contactsScopes, 'any'),
    safeWrap(ingestAkritesExternalContactDetail),
  )
  router.use('/contacts', contactsSubRouter)

  // Same underlying data as advisories above, same scopes: read:akrites-advisories,
  // or Self Serve's read:packages as a fallback until Akrites cuts over.
  const blastRadiusSubRouter = Router()
  blastRadiusSubRouter.use(requireScopes(advisoriesScopes, 'any'))
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
