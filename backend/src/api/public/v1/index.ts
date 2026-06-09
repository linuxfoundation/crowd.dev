import { Router } from 'express'

import { NotFoundError } from '@crowd/common'

import { safeWrap } from '@/middlewares/errorMiddleware'
import { SCOPES } from '@/security/scopes'

import { AUTH0_CONFIG } from '../../../conf'
import { oauth2Middleware } from '../middlewares/oauth2Middleware'
import { requireScopes } from '../middlewares/requireScopes'
import { staticApiKeyMiddleware } from '../middlewares/staticApiKeyMiddleware'

import { memberOrganizationAffiliationsRouter } from './affiliations'
import { membersRouter } from './members'
import { organizationsRouter } from './organizations'
import { packagesRouter } from './packages'
import { batchGetStewardship } from './packages/batchGetStewardship'

export function v1Router(): Router {
  const router = Router()

  router.use('/members', oauth2Middleware(AUTH0_CONFIG), membersRouter())
  router.use('/organizations', oauth2Middleware(AUTH0_CONFIG), organizationsRouter())
  router.use('/affiliations', staticApiKeyMiddleware(), memberOrganizationAffiliationsRouter())

  router.post(
    '/packages\\:batch-stewardship',
    oauth2Middleware(AUTH0_CONFIG),
    requireScopes([SCOPES.READ_STEWARDSHIPS]),
    safeWrap(batchGetStewardship),
  )
  router.use('/packages', oauth2Middleware(AUTH0_CONFIG), packagesRouter())

  router.use(() => {
    throw new NotFoundError()
  })

  return router
}
