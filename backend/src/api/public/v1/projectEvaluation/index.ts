import { Router } from 'express'

import { safeWrap } from '@/middlewares/errorMiddleware'
import { SCOPES } from '@/security/scopes'

import { requireScopes } from '../../middlewares/requireScopes'
import projectEvaluationExec from './projectEvaluationExec'

export function projectEvaluationRouter(): Router {
  const router = Router()

  router.post(
    '/',
    requireScopes([SCOPES.WRITE_PROJECT_EVALUATION]),
    safeWrap(projectEvaluationExec),
  )

  return router
}
