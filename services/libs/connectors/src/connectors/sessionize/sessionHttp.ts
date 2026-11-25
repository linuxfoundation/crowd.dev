import { getServiceChildLogger } from '@crowd/logging'

import { createHttpClient } from '../../http/client'
import type { ConnectorHttp, IPooledToken } from '../../http/client'
import { ProviderAuthError, RateLimitError } from '../../http/errors'
import type { Credential } from '../../types'
import {
  SESSIONIZE_POOL_ENTRY_ID,
  applySessionizeCookie,
  interpretSessionizeResponse,
  signInToSessionize,
} from './session'

const log = getServiceChildLogger('sessionizeDiscover')

function createRateLimitGate(): {
  check: () => void
  park: (resumeAt: Date) => void
} {
  let rateLimitedUntil: Date | null = null
  return {
    check: () => {
      if (rateLimitedUntil && rateLimitedUntil.getTime() <= Date.now()) {
        rateLimitedUntil = null
      }
      if (rateLimitedUntil) {
        throw new RateLimitError('sessionize rate limited the organizer console', {
          resumeAt: rateLimitedUntil,
        })
      }
    },
    park: (resumeAt) => {
      rateLimitedUntil = resumeAt
    },
  }
}

export function createSessionizeSessionHttp(credential: Credential): ConnectorHttp {
  let cookie: string | null = null
  const rateLimit = createRateLimitGate()

  return createHttpClient({
    acquireToken: async (): Promise<IPooledToken> => {
      rateLimit.check()
      if (!cookie) {
        cookie = (await signInToSessionize(credential)).token
      }
      return { id: SESSIONIZE_POOL_ENTRY_ID, value: cookie }
    },
    parkToken: async (_tokenId, resumeAt) => rateLimit.park(resumeAt),
    invalidateToken: async () => {
      cookie = null
    },
    applyToken: applySessionizeCookie,
    interpretResponse: interpretSessionizeResponse,
    log,
  })
}

export function createSessionizeFixedSessionHttp(cookie: string): ConnectorHttp {
  let rejected = false
  const rateLimit = createRateLimitGate()

  return createHttpClient({
    acquireToken: async (): Promise<IPooledToken> => {
      if (rejected) {
        throw new ProviderAuthError(
          'sessionize rejected the session and this client does not renew it',
        )
      }
      rateLimit.check()
      return { id: SESSIONIZE_POOL_ENTRY_ID, value: cookie }
    },
    parkToken: async (_tokenId, resumeAt) => rateLimit.park(resumeAt),
    invalidateToken: async () => {
      rejected = true
    },
    applyToken: applySessionizeCookie,
    interpretResponse: interpretSessionizeResponse,
    log,
  })
}
