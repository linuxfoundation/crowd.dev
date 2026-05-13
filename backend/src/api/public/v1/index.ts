import { Router } from 'express'

import { NotFoundError } from '@crowd/common'

import { AUTH0_CONFIG } from '../../../conf'
import { oauth2Middleware } from '../middlewares/oauth2Middleware'
import { staticApiKeyMiddleware } from '../middlewares/staticApiKeyMiddleware'

import { memberOrganizationAffiliationsRouter } from './affiliations'
import { membersRouter } from './members'
import { organizationsRouter } from './organizations'

export function v1Router(): Router {
  const router = Router()

  router.use('/members', oauth2Middleware(AUTH0_CONFIG), membersRouter())
  router.use('/organizations', oauth2Middleware(AUTH0_CONFIG), organizationsRouter())
  router.use('/affiliations', staticApiKeyMiddleware(), memberOrganizationAffiliationsRouter())

  router.use(() => {
    throw new NotFoundError()
  })

  return router
}
