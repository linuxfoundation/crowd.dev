import crypto from 'crypto'
import type { NextFunction, Request, RequestHandler, Response } from 'express'
import { UnauthorizedError } from '@crowd/common'
import { findApiKeyByHash, optionsQx, touchApiKeyLastUsed } from '@crowd/data-access-layer'

export function staticApiKeyMiddleware(): RequestHandler {
  return async (req: Request, _res: Response, next: NextFunction): Promise<void> => {
    try {
      const authHeader = req.headers.authorization

      if (!authHeader || !authHeader.startsWith('Bearer ')) {
        next(new UnauthorizedError('Missing or invalid Authorization header'))
        return
      }

      const providedKey = authHeader.slice('Bearer '.length)
      const keyHash = crypto.createHash('sha256').update(providedKey).digest('hex')

      const qx = optionsQx(req)
      const apiKey = await findApiKeyByHash(qx, keyHash)

      if (!apiKey) {
        next(new UnauthorizedError('Invalid API key'))
        return
      }

      if (apiKey.revokedAt) {
        next(new UnauthorizedError('API key has been revoked'))
        return
      }

      if (apiKey.expiresAt && apiKey.expiresAt < new Date()) {
        next(new UnauthorizedError('API key has expired'))
        return
      }

      // fire and forget — don't block the request
      touchApiKeyLastUsed(qx, apiKey.id).catch(() => {})

      req.actor = { id: apiKey.name, type: 'service', scopes: apiKey.scopes }

      next()
    } catch (err) {
      next(err)
    }
  }
}
