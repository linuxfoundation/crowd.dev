import axios, { AxiosHeaders } from 'axios'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { createHttpClient } from '../../http/client'
import type { IPooledToken } from '../../http/client'
import { ProviderAuthError, ProviderUnavailableError } from '../../http/errors'
import type { TokenPool } from '../../pool/tokenPool'
import type { Credential } from '../../types'
import {
  SESSIONIZE_POOL_ENTRY_ID,
  SESSIONIZE_SESSION_LIFETIME_MS,
  SESSIONIZE_SIGN_IN_MAX_ATTEMPTS,
  applySessionizeCookie,
  interpretSessionizeResponse,
  prepareSessionizePool,
  signInToSessionize,
} from './session'

vi.mock('@crowd/common', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/common')>()),
  timeout: vi.fn().mockResolvedValue(undefined),
}))

const logMocks = vi.hoisted(() => ({ info: vi.fn(), warn: vi.fn(), error: vi.fn() }))
vi.mock('@crowd/logging', () => ({
  getServiceChildLogger: () => ({ debug: vi.fn(), ...logMocks }),
}))

const PASSWORD = 'hunter2-organizer-password'
const CREDENTIAL: Credential = {
  platform: 'sessionize',
  kind: 'sessionize-organizer-account',
  data: { username: 'organizer@example.org', password: PASSWORD },
}

const LOGIN_PAGE = `
  <form action="/login/other" method="post">
    <input name="__RequestVerificationToken" type="hidden" value="wrong-form-token" />
  </form>
  <form action="/login/classic" method="post">
    <input name="__RequestVerificationToken" type="hidden" value="form-token" />
  </form>`

const mockedRequest = vi.spyOn(axios, 'request')

function reply(status: number, data: unknown = '', headers: Record<string, unknown> = {}) {
  return { status, data, headers, statusText: '', config: {} as never }
}

const loginPage = () =>
  reply(200, LOGIN_PAGE, { 'set-cookie': ['__RequestVerificationToken=af-cookie; path=/; secure'] })

const signedIn = () =>
  reply(302, '', {
    location: '/app/organizer/events',
    'set-cookie': [
      '__Host-Sessionize.Auth=auth-cookie; expires=Thu, 03 Dec 2026 10:00:00 GMT; path=/; secure',
      'ARRAffinity=node-1; path=/',
    ],
  })

function expectNoSecret(value: unknown): void {
  expect(JSON.stringify(value) ?? '').not.toContain(PASSWORD)
  for (const call of [...logMocks.info.mock.calls, ...logMocks.warn.mock.calls]) {
    expect(JSON.stringify(call)).not.toContain(PASSWORD)
  }
}

beforeEach(() => {
  mockedRequest.mockReset()
  Object.values(logMocks).forEach((mock) => mock.mockReset())
  vi.useFakeTimers()
  vi.setSystemTime(new Date('2026-11-19T10:00:00.000Z'))
})

afterEach(() => {
  vi.useRealTimers()
})

describe('signInToSessionize', () => {
  it('posts the classic form with the anti-forgery pair and returns every session cookie for a week', async () => {
    mockedRequest.mockResolvedValueOnce(loginPage()).mockResolvedValueOnce(signedIn())

    const session = await signInToSessionize(CREDENTIAL)

    expect(session).toEqual({
      token:
        '__RequestVerificationToken=af-cookie; __Host-Sessionize.Auth=auth-cookie; ARRAffinity=node-1',
      expiresAt: new Date(Date.now() + SESSIONIZE_SESSION_LIFETIME_MS).toISOString(),
    })
    const post = mockedRequest.mock.calls[1][0]
    expect(post).toMatchObject({
      method: 'POST',
      url: '/login/classic',
      baseURL: 'https://sessionize.com',
      maxRedirects: 0,
      headers: { Cookie: '__RequestVerificationToken=af-cookie' },
    })
    expect(Object.fromEntries(new URLSearchParams(post.data as string))).toEqual({
      __RequestVerificationToken: 'form-token',
      ReturnUrl: '/app/organizer/events',
      Username: 'organizer@example.org',
      Password: PASSWORD,
      RememberMe: 'true',
    })
  })

  it('fails a refused sign-in once, as an auth error that carries no secret', async () => {
    mockedRequest.mockResolvedValueOnce(loginPage()).mockResolvedValueOnce(reply(200, LOGIN_PAGE))

    const err = await signInToSessionize(CREDENTIAL).catch((e: unknown) => e)

    expect(err).toBeInstanceOf(ProviderAuthError)
    expect(mockedRequest).toHaveBeenCalledTimes(2)
    expectNoSecret(err)
  })

  it.each([
    [
      'a login page without the token',
      reply(200, '<form action="/login/classic"></form>'),
      null,
      1,
    ],
    ['a 403 on the login page', reply(403, 'blocked'), null, 1],
    [
      'a 200 challenge page after posting',
      loginPage(),
      reply(200, '<h1>Checking your browser</h1>'),
      2,
    ],
  ])(
    'keeps %s transient so the pool never quarantines the entry',
    async (_, getPage, postPage, requestsPerAttempt) => {
      mockedRequest.mockImplementation(async (config) =>
        config.method === 'POST' ? postPage : getPage,
      )

      await expect(signInToSessionize(CREDENTIAL)).rejects.toBeInstanceOf(ProviderUnavailableError)
      expect(mockedRequest).toHaveBeenCalledTimes(
        SESSIONIZE_SIGN_IN_MAX_ATTEMPTS * requestsPerAttempt,
      )
    },
  )

  it('retries a transient failure, then gives up without leaking the posted password', async () => {
    const networkError = new axios.AxiosError('socket hang up', 'ECONNRESET', {
      data: `Password=${PASSWORD}`,
      headers: new AxiosHeaders(),
    })
    mockedRequest.mockImplementation(async (config) => {
      if (config.method === 'GET') return loginPage()
      throw networkError
    })

    const err = await signInToSessionize(CREDENTIAL).catch((e: unknown) => e)

    expect(err).toBeInstanceOf(ProviderUnavailableError)
    expect(mockedRequest).toHaveBeenCalledTimes(SESSIONIZE_SIGN_IN_MAX_ATTEMPTS * 2)
    expect(logMocks.warn).toHaveBeenCalledTimes(SESSIONIZE_SIGN_IN_MAX_ATTEMPTS)
    expectNoSecret(err)
  })

  it('rejects a credential of the wrong kind before calling sessionize', async () => {
    const credly: Credential = {
      platform: 'credly',
      kind: 'credly-oauth-client',
      data: { clientId: 'id', clientSecret: 'secret', organizationId: 'org' },
    }

    await expect(signInToSessionize(credly)).rejects.toThrow(
      'expected sessionize-organizer-account credential, got credly-oauth-client',
    )
    expect(mockedRequest).not.toHaveBeenCalled()
  })
})

describe('interpretSessionizeResponse', () => {
  it.each([
    ['/login?ReturnUrl=%2Fapp%2Forganizer%2Fevents', true],
    ['https://sessionize.com/login', true],
    ['/Login/classic', true],
    ['/app/organizer/event/26540', false],
    ['https://example.org/login', false],
  ])('treats a redirect to %s as a rejected session: %s', (location, rejected) => {
    const error = interpretSessionizeResponse({ status: 302, headers: { location }, data: '' })

    expect(error instanceof ProviderAuthError).toBe(rejected)
  })

  it('leaves non-redirect responses to the default classification', () => {
    expect(interpretSessionizeResponse({ status: 200, headers: {}, data: '' })).toBeNull()
  })

  it.each([
    ['a text', LOGIN_PAGE],
    ['a binary', Buffer.from(LOGIN_PAGE)],
  ])('treats %s sign-in page served as a 200 as a rejected session', (_, data) => {
    const error = interpretSessionizeResponse({ status: 200, headers: {}, data })

    expect(error).toBeInstanceOf(ProviderAuthError)
  })

  it.each([
    ['a spreadsheet', Buffer.concat([Buffer.from('PK\x03\x04'), Buffer.alloc(64)])],
    ['the events page', '<div id="tab-activated"><table></table></div>'],
  ])('accepts %s served as a 200', (_, data) => {
    expect(interpretSessionizeResponse({ status: 200, headers: {}, data })).toBeNull()
  })

  it('inspects only the first 8 KB of a binary body', () => {
    const data = Buffer.concat([Buffer.alloc(8 * 1024, ' '), Buffer.from(LOGIN_PAGE)])

    expect(interpretSessionizeResponse({ status: 200, headers: {}, data })).toBeNull()
  })
})

describe('session renewal through the http client', () => {
  it('signs in again when sessionize redirects an expired session to the login page', async () => {
    let stored: string | null = 'expired-cookie'
    const invalidateToken = vi.fn(async () => {
      stored = null
    })
    const http = createHttpClient({
      acquireToken: async (): Promise<IPooledToken> => {
        stored ??= (await signInToSessionize(CREDENTIAL)).token
        return { id: SESSIONIZE_POOL_ENTRY_ID, value: stored }
      },
      parkToken: vi.fn(),
      invalidateToken,
      applyToken: applySessionizeCookie,
      interpretResponse: interpretSessionizeResponse,
      log: logMocks as never,
    })
    mockedRequest
      .mockResolvedValueOnce(
        reply(302, '', { location: '/login?ReturnUrl=%2Fapp%2Forganizer%2Fevents' }),
      )
      .mockResolvedValueOnce(loginPage())
      .mockResolvedValueOnce(signedIn())
      .mockResolvedValueOnce(reply(200, 'events'))

    const body = await http.request<string>({ url: 'https://sessionize.com/app/organizer/events' })

    expect(body).toBe('events')
    expect(invalidateToken).toHaveBeenCalledWith(SESSIONIZE_POOL_ENTRY_ID)
    const [expired, , , renewed] = mockedRequest.mock.calls.map(([config]) => config)
    expect(AxiosHeaders.from(expired.headers).get('Cookie')).toBe('expired-cookie')
    expect(AxiosHeaders.from(renewed.headers).get('Cookie')).toContain(
      '__Host-Sessionize.Auth=auth-cookie',
    )
    expect(renewed.maxRedirects).toBe(0)
  })

  it('keeps the session cookie out of the error when sessionize does not answer', async () => {
    const http = createHttpClient({
      acquireToken: async () => ({ id: SESSIONIZE_POOL_ENTRY_ID, value: 'live-session-cookie' }),
      parkToken: vi.fn(),
      invalidateToken: vi.fn(),
      applyToken: applySessionizeCookie,
      interpretResponse: interpretSessionizeResponse,
      log: logMocks as never,
    })
    mockedRequest.mockImplementation(async (config) => {
      throw new axios.AxiosError('timeout of 60000ms exceeded', 'ECONNABORTED', config as never)
    })

    const err = await http
      .request({ url: 'https://sessionize.com/app/organizer/events' }, undefined, 1)
      .catch((e: unknown) => e)

    expect(err).toBeInstanceOf(ProviderUnavailableError)
    expect(JSON.stringify(err)).not.toContain('live-session-cookie')
  })
})

describe('prepareSessionizePool', () => {
  it('seeds the single organizer entry and prefers it', async () => {
    const pool = { seed: vi.fn() } as unknown as TokenPool

    const preparation = await prepareSessionizePool(CREDENTIAL, pool)

    expect(pool.seed).toHaveBeenCalledWith([SESSIONIZE_POOL_ENTRY_ID])
    expect(preparation).toEqual({ preferredEntryId: SESSIONIZE_POOL_ENTRY_ID })
  })
})
