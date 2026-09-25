import { Router } from 'express'

import { safeWrap } from '@/middlewares/errorMiddleware'
import { SCOPES } from '@/security/scopes'

import { requireScopes } from '../../middlewares/requireScopes'
import projectCatalogExec from './projectCatalogExec'

export function projectCatalogRouter(): Router {
  const router = Router()

  router.post('/', requireScopes([SCOPES.WRITE_PROJECT_CATALOG]), safeWrap(projectCatalogExec))

  return router
}
