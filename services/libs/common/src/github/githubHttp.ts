import { GithubPublicClientError } from './types'

export async function doFetch(url: string, init: RequestInit): Promise<Response> {
  try {
    return await fetch(url, init)
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err)
    throw new GithubPublicClientError('TRANSIENT', `Network error calling ${url}: ${message}`)
  }
}

export function rateLimitResetMs(headers: Headers): number {
  const retryAfterSec = parseInt(headers.get('retry-after') ?? '0', 10)
  const resetSec = parseInt(headers.get('x-ratelimit-reset') ?? '0', 10)
  if (retryAfterSec) return Date.now() + retryAfterSec * 1000
  if (resetSec) return resetSec * 1000 + 5_000
  return Date.now() + 65_000
}

// A 401 here is our own token, never a repo signal — it must propagate, not be swallowed.
export function assertAuthenticated(response: Response, url: string): void {
  if (response.status === 401) {
    throw new GithubPublicClientError('AUTH', `401 Unauthorized calling GitHub for ${url}`)
  }
}

const ACCESS_DENIED_MESSAGES = ['ip allow list', 'saml enforcement', 'repository access blocked']

// GitHub reports both primary (403) and secondary (429) rate limits; a bare 403 can also be an
// org policy (IP allow list, SAML) or a genuine permission problem, so only 403 needs the body sniff.
export async function assertNotRateLimited(response: Response, what: string): Promise<void> {
  if (response.status === 429) {
    throw new GithubPublicClientError(
      'RATE_LIMIT',
      `Rate limited ${what}`,
      rateLimitResetMs(response.headers),
    )
  }
  if (response.status === 403) {
    const body = await response.text()
    const lowerBody = body.toLowerCase()
    if (lowerBody.includes('rate limit')) {
      throw new GithubPublicClientError(
        'RATE_LIMIT',
        `Rate limited ${what}`,
        rateLimitResetMs(response.headers),
      )
    }
    const kind = ACCESS_DENIED_MESSAGES.some((message) => lowerBody.includes(message))
      ? 'ACCESS_DENIED'
      : 'AUTH'
    throw new GithubPublicClientError(kind, `403 Forbidden ${what}: ${body.slice(0, 200)}`)
  }
}

export async function readJson<T>(response: Response, url: string): Promise<T> {
  try {
    return (await response.json()) as T
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err)
    throw new GithubPublicClientError('TRANSIENT', `Unreadable response from ${url}: ${message}`)
  }
}
