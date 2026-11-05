import axios, { AxiosError } from 'axios'

import { timeout } from '@crowd/common'
import { getServiceChildLogger } from '@crowd/logging'

import { summarizeBody } from '../../http/client'
import { ConnectorError, ProviderContractError, errorFromHttpStatus } from '../../http/errors'
import type { TokenMinter, TokenPool } from '../../pool/tokenPool'
import { assertCredentialKind } from '../../types'
import type { Credential, PoolPreparation } from '../../types'

export const CREDLY_OAUTH_TOKEN_URL = 'https://api.credly.com/oauth/token'
export const CREDLY_OAUTH_SCOPE = 'issued_badges badge_templates'
export const CREDLY_POOL_ENTRY_ID = 'credly-org'
export const CREDLY_REQUEST_TIMEOUT_MS = 30_000
export const CREDLY_OAUTH_MAX_ATTEMPTS = 3
export const CREDLY_OAUTH_MAX_RETRY_WAIT_MS = 60_000

const RETRY_BACKOFF_BASE_MS = 2_000
const TRANSIENT_ERROR_CLASSES = new Set(['provider.unavailable', 'provider.rate_limit'])

const log = getServiceChildLogger('credlyOAuth')

interface OAuthTokenPayload {
  access_token?: unknown
  expires_in?: unknown
}

type CredlyOAuthCredential = Extract<Credential, { kind: 'credly-oauth-client' }>

export async function mintCredlyAccessToken(
  credential: Credential,
): Promise<{ token: string; expiresAt: string }> {
  assertCredentialKind(credential, 'credly-oauth-client')
  let lastError: ConnectorError | undefined
  for (let attempt = 1; attempt <= CREDLY_OAUTH_MAX_ATTEMPTS; attempt++) {
    try {
      const minted = await requestAccessToken(credential)
      if (attempt > 1) {
        log.warn(
          { event: 'credly_oauth_mint_recovered', attempt },
          'credly oauth token minted after transient failures',
        )
      }
      return minted
    } catch (err) {
      if (!isTransient(err)) {
        throw err
      }
      lastError = err
      const waitMs = retryWaitMs(err, attempt)
      log.warn(
        {
          event: 'credly_oauth_mint_retry',
          attempt,
          maxAttempts: CREDLY_OAUTH_MAX_ATTEMPTS,
          errorClass: err.errorClass,
          status: err.options?.status,
          resumeAt: err.options?.resumeAt?.toISOString(),
          waitMs,
          errMsg: err.message,
        },
        'credly oauth token request failed with a transient error',
      )
      if (waitMs > CREDLY_OAUTH_MAX_RETRY_WAIT_MS) {
        break
      }
      if (attempt < CREDLY_OAUTH_MAX_ATTEMPTS) {
        await timeout(waitMs)
      }
    }
  }
  log.error(
    { event: 'credly_oauth_mint_exhausted', errorClass: lastError?.errorClass },
    'credly oauth token request kept failing, giving up',
  )
  throw lastError
}

export async function prepareCredlyPool(
  credential: Credential,
  pool: TokenPool,
): Promise<PoolPreparation> {
  assertCredentialKind(credential, 'credly-oauth-client')
  await pool.seed([CREDLY_POOL_ENTRY_ID])
  return { preferredEntryId: CREDLY_POOL_ENTRY_ID }
}

export function createCredlyTokenMinter(credential: Credential): TokenMinter {
  assertCredentialKind(credential, 'credly-oauth-client')
  return () => mintCredlyAccessToken(credential)
}

async function requestAccessToken(
  credential: CredlyOAuthCredential,
): Promise<{ token: string; expiresAt: string }> {
  const form = new URLSearchParams({
    grant_type: 'client_credentials',
    client_id: credential.data.clientId,
    client_secret: credential.data.clientSecret,
    scope: CREDLY_OAUTH_SCOPE,
  })
  const mintedAtMs = Date.now()
  let payload: OAuthTokenPayload
  try {
    const response = await axios.post<OAuthTokenPayload>(CREDLY_OAUTH_TOKEN_URL, form.toString(), {
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      timeout: CREDLY_REQUEST_TIMEOUT_MS,
    })
    payload = response.data
  } catch (err) {
    throw classifyTokenError(err) ?? err
  }

  const token = payload?.access_token
  const expiresIn = payload?.expires_in
  if (typeof token !== 'string' || token === '' || !isPositiveNumber(expiresIn)) {
    throw new ProviderContractError(
      'credly oauth token response has no usable access_token or expires_in',
    ).withContext({
      request: { method: 'POST', url: CREDLY_OAUTH_TOKEN_URL },
      response: { status: 200, body: summarizeBody(describeBodyShape(payload)) },
    })
  }
  return { token, expiresAt: new Date(mintedAtMs + expiresIn * 1000).toISOString() }
}

function isTransient(err: unknown): err is ConnectorError {
  return err instanceof ConnectorError && TRANSIENT_ERROR_CLASSES.has(err.errorClass)
}

function retryWaitMs(err: ConnectorError, attempt: number): number {
  const backoffMs = RETRY_BACKOFF_BASE_MS * 2 ** (attempt - 1)
  const resumeAtMs = err.options?.resumeAt?.getTime()
  return resumeAtMs === undefined ? backoffMs : Math.max(backoffMs, resumeAtMs - Date.now())
}

function isPositiveNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value) && value > 0
}

function classifyTokenError(err: unknown): ConnectorError | null {
  if (!axios.isAxiosError(err)) {
    return null
  }
  const status = err.response?.status
  const body = err.response?.data as { error?: string; error_description?: string } | undefined
  const detail = body?.error_description ?? body?.error ?? err.message
  const statusLabel = status === undefined ? 'no response' : `status ${status}`
  return errorFromHttpStatus(
    status,
    `credly oauth token request failed (${statusLabel}): ${detail}`,
    {
      cause: causeWithoutSecrets(err),
      resumeAt: status === 429 ? retryAfterDeadline(err.response?.headers) : undefined,
    },
  ).withContext({
    request: { method: 'POST', url: CREDLY_OAUTH_TOKEN_URL },
    response:
      status === undefined ? undefined : { status, body: summarizeBody(describeBodyShape(body)) },
  })
}

function retryAfterDeadline(headers: unknown): Date | undefined {
  const raw = headerValue(headers, 'retry-after')
  if (!raw) return undefined
  const seconds = Number(raw)
  if (Number.isFinite(seconds) && seconds > 0) {
    return new Date(Date.now() + seconds * 1000)
  }
  const dateMs = Date.parse(raw)
  return Number.isFinite(dateMs) && dateMs > Date.now() ? new Date(dateMs) : undefined
}

function headerValue(headers: unknown, name: string): string | undefined {
  if (!headers || typeof headers !== 'object') return undefined
  const entry = Object.entries(headers as Record<string, unknown>).find(
    ([key]) => key.toLowerCase() === name,
  )
  return typeof entry?.[1] === 'string' ? entry[1] : undefined
}

function causeWithoutSecrets(err: AxiosError): { code?: string; message: string } {
  return { code: err.code, message: err.message }
}

// Only the shape reaches error context: a token could sit at any depth of an unexpected body.
function describeBodyShape(payload: unknown): Record<string, unknown> | undefined {
  if (payload === undefined || payload === null) return undefined
  if (Array.isArray(payload)) return { type: 'array', length: payload.length }
  if (typeof payload === 'object') return { type: 'object', keys: Object.keys(payload) }
  return { type: typeof payload }
}
