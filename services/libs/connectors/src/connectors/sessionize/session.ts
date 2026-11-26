import axios, { AxiosHeaders, AxiosRequestConfig } from 'axios'

import { timeout } from '@crowd/common'
import { getServiceChildLogger } from '@crowd/logging'

import type { HttpResponse, IPooledToken } from '../../http/client'
import {
  ConnectorCodeError,
  ConnectorError,
  ProviderAccessDeniedError,
  ProviderAuthError,
  ProviderUnavailableError,
  RateLimitError,
} from '../../http/errors'
import type { TokenMinter, TokenPool } from '../../pool/tokenPool'
import { assertCredentialKind } from '../../types'
import type { Credential, PoolPreparation } from '../../types'

export const SESSIONIZE_BASE_URL = 'https://sessionize.com'
export const SESSIONIZE_AUTH_COOKIE = '__Host-Sessionize.Auth'
export const SESSIONIZE_POOL_ENTRY_ID = 'sessionize-organizer'
export const SESSIONIZE_SESSION_LIFETIME_MS = 7 * 24 * 60 * 60 * 1000
export const SESSIONIZE_SIGN_IN_MAX_ATTEMPTS = 3

const LOGIN_PATH = '/login'
const SIGN_IN_PATH = '/login/classic'
const SIGN_IN_RETURN_PATH = '/app/organizer/events'
const REQUEST_TIMEOUT_MS = 30_000
const RETRY_BACKOFF_BASE_MS = 2_000
const TRANSIENT_ERROR_CLASSES = new Set(['provider.unavailable', 'provider.rate_limit'])
const ANTI_FORGERY_TOKEN_PATTERN =
  /<form[^>]*action="\/login\/classic"[^>]*>[\s\S]*?name="__RequestVerificationToken"[^>]*value="([^"]+)"/
const SIGN_IN_FORM_PATTERN = /<form[^>]*action="\/login\/classic"/
const ZIP_SIGNATURE = Buffer.from([0x50, 0x4b, 0x03, 0x04])

const log = getServiceChildLogger('sessionizeSession')

type SessionizeCredential = Extract<Credential, { kind: 'sessionize-organizer-account' }>

interface SessionizeResponse {
  status: number
  headers: Record<string, unknown>
  data: unknown
}

export async function signInToSessionize(
  credential: Credential,
  maxAttempts = SESSIONIZE_SIGN_IN_MAX_ATTEMPTS,
): Promise<{ token: string; expiresAt: string }> {
  assertCredentialKind(credential, 'sessionize-organizer-account')
  if (!Number.isInteger(maxAttempts) || maxAttempts < 1) {
    throw new ConnectorCodeError(`sessionize sign-in needs at least 1 attempt, got ${maxAttempts}`)
  }
  let lastError: ConnectorError | undefined
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await requestSession(credential)
    } catch (err) {
      if (!isTransient(err)) {
        throw err
      }
      lastError = err
      log.warn(
        {
          event: 'sessionize_sign_in_retry',
          attempt,
          maxAttempts,
          errorClass: err.errorClass,
          status: err.options?.status,
        },
        'sessionize sign-in failed with a transient error',
      )
      if (attempt < maxAttempts) {
        await timeout(RETRY_BACKOFF_BASE_MS * 2 ** (attempt - 1))
      }
    }
  }
  throw lastError
}

export async function prepareSessionizePool(
  credential: Credential,
  pool: TokenPool,
): Promise<PoolPreparation> {
  assertCredentialKind(credential, 'sessionize-organizer-account')
  await pool.seed([SESSIONIZE_POOL_ENTRY_ID])
  return { preferredEntryId: SESSIONIZE_POOL_ENTRY_ID }
}

export function createSessionizeSessionMinter(credential: Credential): TokenMinter {
  assertCredentialKind(credential, 'sessionize-organizer-account')
  return () => signInToSessionize(credential)
}

// Redirects stay off: axios would otherwise follow the expired-session redirect and return the
// login page as a 200.
export function applySessionizeCookie(
  config: AxiosRequestConfig,
  token: IPooledToken,
): AxiosRequestConfig {
  const headers = AxiosHeaders.from(config.headers)
  headers.set('Cookie', token.value)
  return { ...config, headers, maxRedirects: 0 }
}

export function interpretSessionizeResponse(response: HttpResponse): ConnectorError | null {
  if (response.status === 403) {
    return new ProviderAccessDeniedError(
      "sessionize refused access, the account is not on the event's content team",
      { status: response.status },
    )
  }
  if (isLoginRedirect(response.status, response.headers.location)) {
    return new ProviderAuthError('sessionize session rejected, redirected to the login page', {
      status: response.status,
    })
  }
  if (response.status === 200 && servesSignInForm(response.data)) {
    return new ProviderAuthError('sessionize session rejected, served the sign-in page', {
      status: response.status,
    })
  }
  return null
}

async function requestSession(
  credential: SessionizeCredential,
): Promise<{ token: string; expiresAt: string }> {
  const loginPage = await send({ method: 'GET', url: LOGIN_PATH })
  if (loginPage.status !== 200) {
    throw failedStep('GET', LOGIN_PATH, loginPage.status)
  }
  const antiForgeryToken =
    typeof loginPage.data === 'string'
      ? ANTI_FORGERY_TOKEN_PATTERN.exec(loginPage.data)?.[1]
      : undefined
  if (!antiForgeryToken) {
    throw new ProviderUnavailableError(
      'sessionize login page has no anti-forgery token in the sign-in form',
    ).withContext({ request: { method: 'GET', url: LOGIN_PATH }, response: { status: 200 } })
  }

  const cookies = readSetCookies(loginPage.headers)
  const signIn = await send({
    method: 'POST',
    url: SIGN_IN_PATH,
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      Cookie: cookieHeader(cookies),
    },
    data: new URLSearchParams({
      __RequestVerificationToken: antiForgeryToken,
      ReturnUrl: SIGN_IN_RETURN_PATH,
      Username: credential.data.username,
      Password: credential.data.password,
      RememberMe: 'true',
    }).toString(),
  })
  for (const [name, value] of readSetCookies(signIn.headers)) {
    cookies.set(name, value)
  }

  if (signIn.status === 302 && cookies.get(SESSIONIZE_AUTH_COOKIE)) {
    return {
      token: cookieHeader(cookies),
      expiresAt: new Date(Date.now() + SESSIONIZE_SESSION_LIFETIME_MS).toISOString(),
    }
  }
  if (isSignInRefusal(signIn)) {
    throw new ProviderAuthError('sessionize refused the organizer credentials', {
      status: signIn.status,
    }).withContext({ request: { method: 'POST', url: SIGN_IN_PATH } })
  }
  throw failedStep('POST', SIGN_IN_PATH, signIn.status)
}

async function send(config: AxiosRequestConfig): Promise<SessionizeResponse> {
  try {
    return await axios.request({
      ...config,
      baseURL: SESSIONIZE_BASE_URL,
      timeout: REQUEST_TIMEOUT_MS,
      maxRedirects: 0,
      responseType: 'text',
      validateStatus: () => true,
    })
  } catch (err) {
    if (!axios.isAxiosError(err)) {
      throw err
    }
    throw new ProviderUnavailableError(
      `sessionize ${config.method} ${config.url} got no response`,
      {
        cause: { code: err.code, message: err.message },
      },
    ).withContext({ request: { method: String(config.method), url: String(config.url) } })
  }
}

// Only a refused sign-in may be provider.auth: the pool quarantines the single entry for good on
// auth and contract errors, so an outage or a WAF page must stay transient.
function failedStep(method: string, url: string, status: number): ConnectorError {
  const message = `sessionize ${method} ${url} failed (status ${status})`
  const error =
    status === 429
      ? new RateLimitError(message, { status })
      : new ProviderUnavailableError(message, { status })
  return error.withContext({ request: { method, url }, response: { status } })
}

function readSetCookies(headers: Record<string, unknown>): Map<string, string> {
  const raw = headers['set-cookie']
  const lines = Array.isArray(raw) ? raw : typeof raw === 'string' ? [raw] : []
  const cookies = new Map<string, string>()
  for (const line of lines) {
    const pair = String(line).split(';')[0]
    const separator = pair.indexOf('=')
    if (separator > 0) {
      cookies.set(pair.slice(0, separator).trim(), pair.slice(separator + 1).trim())
    }
  }
  return cookies
}

function cookieHeader(cookies: Map<string, string>): string {
  return [...cookies]
    .filter(([, value]) => value !== '')
    .map(([name, value]) => `${name}=${value}`)
    .join('; ')
}

function isSignInRefusal(response: SessionizeResponse): boolean {
  return (
    response.status === 200 &&
    typeof response.data === 'string' &&
    SIGN_IN_FORM_PATTERN.test(response.data)
  )
}

function servesSignInForm(data: unknown): boolean {
  if (typeof data === 'string') {
    return SIGN_IN_FORM_PATTERN.test(data)
  }
  if (Buffer.isBuffer(data)) {
    if (data.subarray(0, ZIP_SIGNATURE.length).equals(ZIP_SIGNATURE)) {
      return false
    }
    return SIGN_IN_FORM_PATTERN.test(data.toString('utf8'))
  }
  return false
}

function isLoginRedirect(status: number, location: string | undefined): boolean {
  return (
    status >= 300 && status < 400 && /^(https:\/\/sessionize\.com)?\/login\b/i.test(location ?? '')
  )
}

function isTransient(err: unknown): err is ConnectorError {
  return err instanceof ConnectorError && TRANSIENT_ERROR_CLASSES.has(err.errorClass)
}
