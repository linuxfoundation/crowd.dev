import { getSecurityContactsConfig } from '../../config'
import { FetchError } from '../../go/types'

// crates.io API client for blast-radius; mirrors go/proxyClient.ts's retry/backoff shape.

const API_BASE = process.env.CRATES_IO_BASE_URL ?? 'https://crates.io'
const STATIC_BASE = process.env.CRATES_STATIC_BASE_URL ?? 'https://static.crates.io'
const MAX_429_RETRIES = 5

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms))
}

async function getWithRetry(url: string, timeoutMs: number): Promise<Response | FetchError> {
  // crates.io rejects requests without an identifying User-Agent.
  const headers = { 'User-Agent': getSecurityContactsConfig().userAgent }

  for (let attempt = 0; attempt <= MAX_429_RETRIES; attempt++) {
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), timeoutMs)

    let res: Response
    try {
      res = await fetch(url, { signal: controller.signal, headers })
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
    return res
  }

  return { kind: 'RATE_LIMIT', statusCode: 429, message: '429 after retries' }
}

// GET /api/v1/crates/{name}/versions — includes yanked versions (still installable/vulnerable).
export async function fetchCrateVersions(
  name: string,
  timeoutMs: number,
): Promise<string[] | FetchError> {
  const url = `${API_BASE}/api/v1/crates/${encodeURIComponent(name)}/versions`

  const res = await getWithRetry(url, timeoutMs)
  if (!('ok' in res)) return res

  let body: { versions?: Array<{ num?: string }> }
  try {
    body = (await res.json()) as { versions?: Array<{ num?: string }> }
  } catch {
    return { kind: 'MALFORMED', message: 'invalid json' }
  }
  if (!Array.isArray(body.versions)) {
    return { kind: 'MALFORMED', message: 'missing versions array' }
  }

  return body.versions.map((v) => v.num).filter((num): num is string => Boolean(num))
}

// GET /api/v1/crates/{name} — lightweight call for latest version (avoids full list).
export async function fetchCrateLatestVersion(
  name: string,
  timeoutMs: number,
): Promise<string | FetchError> {
  const url = `${API_BASE}/api/v1/crates/${encodeURIComponent(name)}`

  const res = await getWithRetry(url, timeoutMs)
  if (!('ok' in res)) return res

  let body: { crate?: { newest_version?: string; max_version?: string } }
  try {
    body = (await res.json()) as { crate?: { newest_version?: string; max_version?: string } }
  } catch {
    return { kind: 'MALFORMED', message: 'invalid json' }
  }
  const version = body.crate?.newest_version ?? body.crate?.max_version
  if (version === undefined || version === null) {
    return { kind: 'MALFORMED', message: 'missing crate.newest_version' }
  }

  return version
}

// static.crates.io serves .crate files at a fixed, predictable path — no API call needed.
export function crateSourceUrl(name: string, version: string): string {
  return `${STATIC_BASE}/crates/${encodeURIComponent(name)}/${encodeURIComponent(name)}-${encodeURIComponent(version)}.crate`
}
