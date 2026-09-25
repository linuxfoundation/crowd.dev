import { Router } from 'express'

import { safeWrap } from '@/middlewares/errorMiddleware'
import { SCOPES } from '@/security/scopes'

import { requireScopes } from '../../middlewares/requireScopes'
import projectOnboardingExec from './projectOnboardingExec'

export function projectOnboardingRouter(): Router {
  const router = Router()

  router.post(
    '/',
    requireScopes([SCOPES.WRITE_PROJECT_ONBOARDING]),
    safeWrap(projectOnboardingExec),
  )

  return router
}
