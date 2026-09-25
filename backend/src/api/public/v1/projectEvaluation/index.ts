import { Router } from 'express'

import { safeWrap } from '@/middlewares/errorMiddleware'

import projectEvaluationExec from './projectEvaluationExec'

export function projectEvaluationRouter(): Router {
  const router = Router()

  router.post('/', safeWrap(projectEvaluationExec))

  return router
}
