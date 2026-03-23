import type { Logger } from '@crowd/logging'
import type { RedisClient } from '@crowd/redis'
import type { Client as TemporalClient } from '@crowd/temporal'

import type ApiResponseHandler from '@/api/apiResponseHandler'
import type { Actor } from '@/types/api'

declare global {
  namespace Express {
    interface Request {
      actor: Actor
      redis: RedisClient
      temporal: TemporalClient
      log: Logger
      responseHandler: ApiResponseHandler
    }
  }
}
