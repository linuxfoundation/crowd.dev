import { getServiceChildLogger } from '@crowd/logging'

import { createHttpClient } from '../../http/client'
import type { IPooledToken } from '../../http/client'
import { RateLimitError } from '../../http/errors'
import { assertCredentialKind } from '../../types'
import type { Channel, Credential } from '../../types'
import { fetchActivatedEvents } from './consoleEvents'
import {
  SESSIONIZE_POOL_ENTRY_ID,
  applySessionizeCookie,
  interpretSessionizeResponse,
  signInToSessionize,
} from './session'

const log = getServiceChildLogger('sessionizeDiscover')

export async function discoverSessionize(credential: Credential): Promise<Channel[]> {
  assertCredentialKind(credential, 'sessionize-organizer-account')
  let cookie: string | null = null
  let rateLimitedUntil: Date | null = null

  const http = createHttpClient({
    acquireToken: async (): Promise<IPooledToken> => {
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

  const events = await fetchActivatedEvents(http)
  return events.map((event) => ({ channelId: event.eventKey, channelName: event.eventName }))
}
