import { getServiceChildLogger } from '@crowd/logging'

import { createHttpClient } from '../../http/client'
import type { IPooledToken } from '../../http/client'
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

  const http = createHttpClient({
    acquireToken: async (): Promise<IPooledToken> => {
      if (!cookie) {
        cookie = (await signInToSessionize(credential)).token
      }
      return { id: SESSIONIZE_POOL_ENTRY_ID, value: cookie }
    },
    parkToken: async () => undefined,
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
