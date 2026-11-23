import type { Request } from 'express'
import rateLimit, { type Store } from 'express-rate-limit'

import { RateLimitError } from '@crowd/common'

export function createRateLimiter({
  max,
  windowMs,
  keyGenerator,
  store,
  skip: additionalSkip,
}: {
  max: number
  windowMs: number
  keyGenerator?: (req: Request) => string
  store?: Store
  skip?: (req: Request) => boolean
}) {
  return rateLimit({
    max,
    windowMs,
    standardHeaders: true,
    ...(keyGenerator ? { keyGenerator } : {}),
    ...(store ? { store } : {}),
    handler: (_req, res) => {
      const err = new RateLimitError()
      res.status(err.status).json(err.toJSON())
    },
    skip: (req) =>
      req.method === 'OPTIONS' ||
      req.originalUrl.endsWith('/import') ||
      (additionalSkip ? additionalSkip(req) : false),
  })
}
