import { Router } from 'express'

import { safeWrap } from '@/middlewares/errorMiddleware'

import projectOnboardingExec from './projectOnboardingExec'

export function projectOnboardingRouter(): Router {
  const router = Router()

  router.post('/', safeWrap(projectOnboardingExec))

  return router
}
