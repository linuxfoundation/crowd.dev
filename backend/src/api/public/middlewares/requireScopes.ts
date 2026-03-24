import type { NextFunction, Request, Response } from 'express'

import { InsufficientScopeError, UnauthorizedError } from '@crowd/common'

import { Scope } from '@/security/scopes'

export const requireScopes =
  (required: Scope[], mode: 'all' | 'any' = 'all') =>
  (req: Request, _res: Response, next: NextFunction) => {
    if (!req.actor) {
      next(new UnauthorizedError())
      return
    }

    const granted = new Set(req.actor.scopes)
    const hasAccess =
      mode === 'all' ? required.every((s) => granted.has(s)) : required.some((s) => granted.has(s))

    if (!hasAccess) {
      next(new InsufficientScopeError())
      return
    }

    next()
  }
