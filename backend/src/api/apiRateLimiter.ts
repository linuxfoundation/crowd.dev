import rateLimit from 'express-rate-limit'

import { RateLimitError } from '@crowd/common'

export function createRateLimiter({ max, windowMs }: { max: number; windowMs: number }) {
  return rateLimit({
    max,
    windowMs,
    handler: (_req, res) => {
      const err = new RateLimitError()
      res.status(err.status).json(err.toJSON())
    },
    skip: (req) => req.method === 'OPTIONS' || req.originalUrl.endsWith('/import'),
  })
}
