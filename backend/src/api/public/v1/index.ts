import { Router } from 'express'

import { membersRouter } from './members'
import { organizationsRouter } from './organizations'

export function v1Router(): Router {
  const router = Router()

  router.use('/members', membersRouter())
  router.use('/organizations', organizationsRouter())

  return router
}
