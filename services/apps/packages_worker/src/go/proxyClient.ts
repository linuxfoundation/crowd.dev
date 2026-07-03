import { FetchError, GoProxyLatest } from './types'

const BASE = process.env.GO_PROXY_BASE_URL ?? 'https://proxy.golang.org'
const ZERO_TIME = '0001-01-01T00:00:00Z'
const MAX_429_RETRIES = 5

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms))
}

// GOPROXY spec: uppercase letters in a module path are escaped as '!' + lowercase.
export function escapeModulePath(module: string): string {
  return module.replace(/!/g, '!!').replace(/[A-Z]/g, (c) => '!' + c.toLowerCase())
}

export async function fetchLatest(
  module: string,
  timeoutMs: number,
): Promise<GoProxyLatest | FetchError> {
  const url = `${BASE}/${escapeModulePath(module)}/@latest`

  for (let attempt = 0; attempt <= MAX_429_RETRIES; attempt++) {
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), timeoutMs)

    let res: Response
    try {
      res = await fetch(url, { signal: controller.signal })
    } catch (e) {
      return { kind: 'TRANSIENT', message: `network error: ${(e as Error).message}` }
    } finally {
      clearTimeout(timer)
    }

    if (res.status === 429) {
      if (attempt === MAX_429_RETRIES) {
        return { kind: 'RATE_LIMIT', statusCode: 429, message: '429 after retries' }
      }
      const retryAfterSec = parseInt(res.headers.get('retry-after') ?? '', 10)
      const waitMs = Number.isNaN(retryAfterSec) ? 1000 * 2 ** attempt : retryAfterSec * 1000
      await sleep(waitMs)
      continue
    }
    // Any other 4xx is permanent (unknown/invalid module path) — skip, don't retry.
    if (res.status >= 400 && res.status < 500) {
      return { kind: 'NOT_FOUND', statusCode: res.status, message: `${res.status}` }
    }
    if (res.status !== 200) {
      return {
        kind: 'TRANSIENT',
        statusCode: res.status,
        message: `unexpected status ${res.status}`,
      }
    }

    let body: { Version?: string; Time?: string; Origin?: { URL?: string } }
    try {
      body = (await res.json()) as { Version?: string; Time?: string; Origin?: { URL?: string } }
    } catch {
      return { kind: 'MALFORMED', message: 'invalid json' }
    }
    if (!body.Version) return { kind: 'MALFORMED', message: 'missing Version' }

    return {
      version: body.Version,
      releaseAt: body.Time && body.Time !== ZERO_TIME ? body.Time : null,
      repoUrl: body.Origin?.URL || null,
    }
  }

  return { kind: 'RATE_LIMIT', statusCode: 429, message: '429 after retries' }
}
