import { Agent, type Dispatcher } from 'undici'

import type {
  FetchError,
  P2Metadata,
  P2NotModified,
  PackagistMinifiedVersion,
  PackagistStatsJson,
} from './types'

// Shared dispatcher for all packagist fetches. autoSelectFamily races IPv4/IPv6
// (Happy Eyeballs) — without it, an environment with broken IPv6 (common in
// containers) hangs on the first resolved AAAA address until UND_ERR_CONNECT_TIMEOUT.
// Connect timeout stays modest; the per-request 30s abort still bounds the whole call.
export const packagistDispatcher: Dispatcher = new Agent({
  connect: { timeout: 15_000, autoSelectFamily: true },
})

export function buildPackagistUserAgent(): string {
  const mailto = process.env.CROWD_PACKAGES_PACKAGIST_MAILTO || 'oss-packages@linuxfoundation.org'
  return `lfx-packages-worker/0.1 (+https://lfx.linuxfoundation.org; mailto=${mailto})`
}

// undici wraps network failures as `TypeError: fetch failed` with the real reason
// (ECONNRESET, ETIMEDOUT, DNS, …) on `cause` — surface it or the logs are undiagnosable.
export function describeFetchFailure(err: unknown): string {
  const cause = (err as { cause?: { message?: string; code?: string } })?.cause
  const detail = cause?.code ?? cause?.message
  return detail ? `${String(err)} (cause: ${detail})` : String(err)
}

export async function fetchPackagistStats(name: string): Promise<PackagistStatsJson | FetchError> {
  const url = `https://packagist.org/packages/${name}.json`
  const abort = new AbortController()
  const timer = setTimeout(() => abort.abort(), 30_000)

  try {
    let res: Response
    try {
      // `dispatcher` is an undici-specific fetch option not present in the DOM RequestInit type.
      const init: RequestInit & { dispatcher?: Dispatcher } = {
        headers: {
          Accept: 'application/json',
          'User-Agent': buildPackagistUserAgent(),
        },
        signal: abort.signal,
        dispatcher: packagistDispatcher,
      }
      res = await fetch(url, init as RequestInit)
    } catch (err) {
      return { kind: 'TRANSIENT', message: describeFetchFailure(err) }
    }

    if (res.status === 404)
      return { kind: 'NOT_FOUND', message: `${name} not found`, statusCode: 404 }
    if (res.status === 429) return { kind: 'RATE_LIMIT', message: 'rate limited', statusCode: 429 }
    if (!res.ok) return { kind: 'TRANSIENT', message: `HTTP ${res.status}`, statusCode: res.status }

    let json: unknown
    try {
      json = await res.json()
    } catch {
      if (abort.signal.aborted) return { kind: 'TRANSIENT', message: 'body read timed out' }
      return { kind: 'MALFORMED', message: 'invalid JSON' }
    }

    // Shape guard: body.package must be an object with a string name
    if (!isPackagistStatsJson(json)) {
      return { kind: 'MALFORMED', message: 'unexpected shape' }
    }

    return json
  } finally {
    clearTimeout(timer)
  }
}

export async function fetchPackagistP2(
  name: string,
  ifModifiedSince: string | null,
): Promise<P2Metadata | P2NotModified | FetchError> {
  const url = `https://repo.packagist.org/p2/${name}.json`
  const abort = new AbortController()
  const timer = setTimeout(() => abort.abort(), 30_000)

  try {
    const headers: Record<string, string> = {
      Accept: 'application/json',
      'User-Agent': buildPackagistUserAgent(),
    }
    if (ifModifiedSince !== null) {
      headers['If-Modified-Since'] = ifModifiedSince
    }

    let res: Response
    try {
      const init: RequestInit & { dispatcher?: Dispatcher } = {
        headers,
        signal: abort.signal,
        dispatcher: packagistDispatcher,
      }
      res = await fetch(url, init as RequestInit)
    } catch (err) {
      return { kind: 'TRANSIENT', message: describeFetchFailure(err) }
    }

    // 304 must be checked first
    if (res.status === 304) {
      return { kind: 'NOT_MODIFIED' }
    }

    if (res.status === 404)
      return { kind: 'NOT_FOUND', message: `${name} not found`, statusCode: 404 }
    if (res.status === 429) return { kind: 'RATE_LIMIT', message: 'rate limited', statusCode: 429 }
    if (!res.ok) return { kind: 'TRANSIENT', message: `HTTP ${res.status}`, statusCode: res.status }

    let json: unknown
    try {
      json = await res.json()
    } catch {
      if (abort.signal.aborted) return { kind: 'TRANSIENT', message: 'body read timed out' }
      return { kind: 'MALFORMED', message: 'invalid JSON' }
    }

    // Shape guard: body.packages[name] must be an array
    if (!isP2Response(json, name)) {
      return { kind: 'MALFORMED', message: 'unexpected shape' }
    }

    const minifiedVersions = json.packages[name] as PackagistMinifiedVersion[]
    const lastModified = res.headers.get('last-modified') ?? null

    return {
      minifiedVersions,
      lastModified,
    }
  } finally {
    clearTimeout(timer)
  }
}

function isPackagistStatsJson(v: unknown): v is PackagistStatsJson {
  if (typeof v !== 'object' || v === null || !('package' in v)) return false
  const pkg = (v as { package: unknown }).package
  return (
    typeof pkg === 'object' && pkg !== null && typeof (pkg as { name?: unknown }).name === 'string'
  )
}

function isP2Response(v: unknown, name: string): v is { packages: Record<string, unknown[]> } {
  if (typeof v !== 'object' || v === null || !('packages' in v)) return false
  const packages = (v as { packages: unknown }).packages
  if (typeof packages !== 'object' || packages === null) return false
  const entries = (packages as Record<string, unknown>)[name]
  return Array.isArray(entries)
}
