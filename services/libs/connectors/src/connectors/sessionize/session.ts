import axios, { AxiosHeaders, AxiosRequestConfig } from 'axios'

import { timeout } from '@crowd/common'
import { getServiceChildLogger } from '@crowd/logging'

import type { HttpResponse, IPooledToken } from '../../http/client'
import {
  ConnectorError,
  ProviderAuthError,
  ProviderContractError,
  ProviderUnavailableError,
  errorFromHttpStatus,
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

const log = getServiceChildLogger('sessionizeSession')

type SessionizeCredential = Extract<Credential, { kind: 'sessionize-organizer-account' }>

interface SessionizeResponse {
  status: number
  headers: Record<string, unknown>
  data: unknown
}

export async function signInToSessionize(
  credential: Credential,
): Promise<{ token: string; expiresAt: string }> {
  assertCredentialKind(credential, 'sessionize-organizer-account')
  let lastError: ConnectorError | undefined
  for (let attempt = 1; attempt <= SESSIONIZE_SIGN_IN_MAX_ATTEMPTS; attempt++) {
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
          maxAttempts: SESSIONIZE_SIGN_IN_MAX_ATTEMPTS,
          errorClass: err.errorClass,
          status: err.options?.status,
        },
        'sessionize sign-in failed with a transient error',
      )
      if (attempt < SESSIONIZE_SIGN_IN_MAX_ATTEMPTS) {
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
  if (isLoginRedirect(response.status, response.headers.location)) {
    return new ProviderAuthError('sessionize session rejected, redirected to the login page', {
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
    throw new ProviderContractError(
      'sessionize login page has no anti-forgery token in the sign-in form',
    ).withContext({ request: { method: 'GET', url: LOGIN_PATH } })
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
  if (signIn.status < 400) {
    throw new ProviderAuthError(
      `sessionize sign-in refused (status ${signIn.status}, no ${SESSIONIZE_AUTH_COOKIE} cookie)`,
      { status: signIn.status },
    ).withContext({ request: { method: 'POST', url: SIGN_IN_PATH } })
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
    const cause = axios.isAxiosError(err) ? { code: err.code, message: err.message } : undefined
    throw new ProviderUnavailableError(
      `sessionize ${config.method} ${config.url} got no response`,
      {
        cause,
      },
    ).withContext({ request: { method: String(config.method), url: String(config.url) } })
  }
}

function failedStep(method: string, url: string, status: number): ConnectorError {
  return errorFromHttpStatus(
    status,
    `sessionize ${method} ${url} failed (status ${status})`,
  ).withContext({ request: { method, url }, response: { status } })
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

function isLoginRedirect(status: number, location: string | undefined): boolean {
  return (
    status >= 300 && status < 400 && /^(https:\/\/sessionize\.com)?\/login\b/i.test(location ?? '')
  )
}

function isTransient(err: unknown): err is ConnectorError {
  return err instanceof ConnectorError && TRANSIENT_ERROR_CLASSES.has(err.errorClass)
}
