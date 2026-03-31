import type { NextFunction, Request, RequestHandler, Response } from 'express'
import { auth } from 'express-oauth2-jwt-bearer'

import { UnauthorizedError } from '@crowd/common'

import type { Auth0Configuration } from '@/conf/configTypes'
import type { Auth0TokenPayload } from '@/types/api'

function resolveIssuer(req: Request): string | undefined {
  const token = req.headers.authorization?.split(' ')[1]
  if (!token) return undefined
  try {
    const { iss } = JSON.parse(Buffer.from(token.split('.')[1], 'base64url').toString())
    return typeof iss === 'string' ? iss : undefined
  } catch {
    return undefined
  }
}

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
  const issuers = config.issuerBaseURLs
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)

  if (issuers.length === 0) {
    throw new Error('No auth0 issuers configured')
  }

  const handlersByIssuer = new Map(
    issuers.map((issuerBaseURL) => [
      issuerBaseURL.replace(/\/$/, ''),
      auth({ issuerBaseURL, audience: config.audience }),
    ]),
  )

  const verifyJwt: RequestHandler = (req, res, next) => {
    const iss = resolveIssuer(req)
    if (!iss) {
      next(new UnauthorizedError('Missing or malformed bearer token'))
      return
    }

    const handler = handlersByIssuer.get(iss.replace(/\/$/, ''))

    if (!handler) {
      next(new UnauthorizedError('Unknown token issuer'))
      return
    }

    handler(req, res, next)
  }

  return [verifyJwt, resolveActor]
}
