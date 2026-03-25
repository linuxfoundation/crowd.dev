import { Router } from 'express'

import { AUTH0_CONFIG } from '../../../conf'

import { oauth2Middleware } from '../middlewares/oauth2Middleware'
import { staticApiKeyMiddleware } from '../middlewares/staticApiKeyMiddleware'
import { devStatsRouter } from './dev-stats'
import { membersRouter } from './members'
import { organizationsRouter } from './organizations'

export function v1Router(): Router {
  const router = Router()

  router.use('/members', oauth2Middleware(AUTH0_CONFIG), membersRouter())
  router.use('/organizations', oauth2Middleware(AUTH0_CONFIG), organizationsRouter())
  router.use('/affiliations', staticApiKeyMiddleware(), devStatsRouter())

  return router
}
