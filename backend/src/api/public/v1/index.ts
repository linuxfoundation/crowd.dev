import { Router } from 'express'

import { NotFoundError } from '@crowd/common'

import { createRateLimiter } from '@/api/apiRateLimiter'
import { safeWrap } from '@/middlewares/errorMiddleware'
import { SCOPES } from '@/security/scopes'

import { AUTH0_CONFIG } from '../../../conf'
import { oauth2Middleware } from '../middlewares/oauth2Middleware'
import { requireScopes } from '../middlewares/requireScopes'
import { staticApiKeyMiddleware } from '../middlewares/staticApiKeyMiddleware'

import { memberOrganizationAffiliationsRouter } from './affiliations'
import { akritesRouter } from './akrites'
import { membersRouter } from './members'
import { organizationsRouter } from './organizations'
import { osspreyRouter } from './ossprey'
import { packagesRouter } from './packages'
import { batchGetStewardship } from './packages/batchGetStewardship'
import { stewardshipsRouter } from './stewardships'

const packagesRateLimiter = createRateLimiter({ max: 60, windowMs: 60 * 1000 })

export function v1Router(): Router {
  const router = Router()

  router.use('/members', oauth2Middleware(AUTH0_CONFIG), membersRouter())
  router.use('/organizations', oauth2Middleware(AUTH0_CONFIG), organizationsRouter())
  router.use('/affiliations', staticApiKeyMiddleware(), memberOrganizationAffiliationsRouter())

  // TODO[deprecate]: /packages, /stewardships, /ossprey are superseded by /akrites — remove once consumers have migrated
  router.post(
    /^\/packages:batch-stewardship\/?$/,
    oauth2Middleware(AUTH0_CONFIG),
    packagesRateLimiter,
    requireScopes([SCOPES.READ_PACKAGES, SCOPES.READ_STEWARDSHIPS], 'all'),
    safeWrap(batchGetStewardship),
  )
  router.use('/packages', oauth2Middleware(AUTH0_CONFIG), packagesRouter())
  router.use('/stewardships', oauth2Middleware(AUTH0_CONFIG), stewardshipsRouter())
  router.use('/ossprey', oauth2Middleware(AUTH0_CONFIG), osspreyRouter())

  router.use('/akrites', oauth2Middleware(AUTH0_CONFIG), akritesRouter())

  router.use(() => {
    throw new NotFoundError()
  })

  return router
}
