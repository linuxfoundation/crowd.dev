import type { Credential, IPooledToken } from '@crowd/connectors'
import { createHttpClient } from '@crowd/connectors'
import {
  SESSIONIZE_AUTH_COOKIE,
  SESSIONIZE_BASE_URL,
  SESSIONIZE_POOL_ENTRY_ID,
  applySessionizeCookie,
  interpretSessionizeResponse,
  signInToSessionize,
} from '@crowd/connectors/src/connectors/sessionize/session'
import { getServiceLogger } from '@crowd/logging'

const log = getServiceLogger()

const EVENTS_URL = `${SESSIONIZE_BASE_URL}/app/organizer/events`
const REJECTED_COOKIE = `${SESSIONIZE_AUTH_COOKIE}=rejected-by-probe`

function credentialFromEnv(): Credential {
  const username = process.env.CROWD_SESSIONIZE_USERNAME
  const password = process.env.CROWD_SESSIONIZE_PASSWORD
  if (!username || !password) {
    log.error('set CROWD_SESSIONIZE_USERNAME and CROWD_SESSIONIZE_PASSWORD first')
    process.exit(1)
  }
  return {
    platform: 'sessionize',
    kind: 'sessionize-organizer-account',
    data: { username, password },
  }
}

function countEvents(page: string): number {
  return new Set([...page.matchAll(/\/app\/organizer\/event\/(\d+)/g)].map((match) => match[1]))
    .size
}

async function main(): Promise<void> {
  const credential = credentialFromEnv()
  let cookie: string | null = null
  let signIns = 0

  const http = createHttpClient({
    acquireToken: async (): Promise<IPooledToken> => {
      if (!cookie) {
        const session = await signInToSessionize(credential)
        signIns += 1
        log.info({ signIns, expiresAt: session.expiresAt }, 'signed in')
        cookie = session.token
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

  const first = await http.request<string>({ url: EVENTS_URL, responseType: 'text' })
  log.info({ events: countEvents(first), signIns }, 'organizer console loaded')

  cookie = REJECTED_COOKIE
  const renewed = await http.request<string>({ url: EVENTS_URL, responseType: 'text' })
  log.info(
    { events: countEvents(renewed), signIns, requests: http.requestCount() },
    'organizer console loaded again after the session was rejected',
  )

  if (signIns !== 2) {
    throw new Error(`expected 2 sign-ins, got ${signIns}`)
  }
}

main().catch((err) => {
  log.error(
    { errorClass: err?.errorClass, errMsg: err?.message },
    'sessionize session probe failed',
  )
  process.exit(1)
})
