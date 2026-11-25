import { getServiceChildLogger } from '@crowd/logging'

import { createHttpClient } from '../../http/client'
import type { ConnectorHttp, IPooledToken } from '../../http/client'
import { RateLimitError } from '../../http/errors'
import type { Credential } from '../../types'
import {
  SESSIONIZE_POOL_ENTRY_ID,
  applySessionizeCookie,
  interpretSessionizeResponse,
  signInToSessionize,
} from './session'

const log = getServiceChildLogger('sessionizeDiscover')

export function createSessionizeSessionHttp(credential: Credential): ConnectorHttp {
  let cookie: string | null = null
  let rateLimitedUntil: Date | null = null

  return createHttpClient({
    acquireToken: async (): Promise<IPooledToken> => {
      if (rateLimitedUntil && rateLimitedUntil.getTime() <= Date.now()) {
        rateLimitedUntil = null
      }
      if (rateLimitedUntil) {
        throw new RateLimitError('sessionize rate limited the organizer console', {
          resumeAt: rateLimitedUntil,
        })
      }
      if (!cookie) {
        cookie = (await signInToSessionize(credential)).token
      }
      return { id: SESSIONIZE_POOL_ENTRY_ID, value: cookie }
    },
    parkToken: async (_tokenId, resumeAt) => {
      rateLimitedUntil = resumeAt
    },
    invalidateToken: async () => {
      cookie = null
    },
    applyToken: applySessionizeCookie,
    interpretResponse: interpretSessionizeResponse,
    log,
  })
}
