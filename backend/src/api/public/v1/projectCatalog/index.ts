import { Router } from 'express'

import { safeWrap } from '@/middlewares/errorMiddleware'

import projectCatalogExec from './projectCatalogExec'

export function projectCatalogRouter(): Router {
  const router = Router()

  router.post('/', safeWrap(projectCatalogExec))

  return router
}
