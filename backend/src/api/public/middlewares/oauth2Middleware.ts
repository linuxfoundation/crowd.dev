import type { NextFunction, Request, RequestHandler, Response } from 'express'
import { auth } from 'express-oauth2-jwt-bearer'

import { UnauthorizedError } from '@crowd/common'

import type { Auth0Configuration } from '@/conf/configTypes'
import type { Auth0TokenPayload } from '@/types/api'

function resolveActor(req: Request, _res: Response, next: NextFunction): void {
  const payload = (req.auth?.payload ?? {}) as Auth0TokenPayload

  const rawId = payload.sub ?? payload.azp

  if (!rawId) {
    next(new UnauthorizedError('Token missing caller identity'))
    return
  }

  const id = rawId.replace(/@clients$/, '')

  const scopes = typeof payload.scope === 'string' ? payload.scope.split(' ').filter(Boolean) : []

  req.actor = { id, type: 'service', scopes }

  next()
}

export function oauth2Middleware(config: Auth0Configuration): RequestHandler[] {
  return [
    auth({
      issuerBaseURL: config.issuerBaseURL,
      audience: config.audience,
    }),
    resolveActor,
  ]
}
