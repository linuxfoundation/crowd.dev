import type { Dispatcher } from 'undici'

import type { FetchError, Packument } from './types'

const REGISTRY = 'https://registry.npmjs.org'
const USER_AGENT = 'lfx-packages-worker/0.1 (+https://lfx.linuxfoundation.org)'

function encodeNpmName(name: string): string {
  return name.startsWith('@') ? `@${encodeURIComponent(name.slice(1))}` : encodeURIComponent(name)
}

// `dispatcher` (an undici ProxyAgent) routes the request through a specific proxy IP
// so concurrent ingest lanes each use their own egress address / rate limit.
export async function fetchPackument(
  name: string,
  dispatcher?: Dispatcher,
): Promise<Packument | FetchError> {
  const url = `${REGISTRY}/${encodeNpmName(name)}`
  const abort = new AbortController()
  const timer = setTimeout(() => abort.abort(), 30_000)
  let res: Response
  try {
    // `dispatcher` is an undici-specific fetch option not present in the DOM RequestInit type.
    const init: RequestInit & { dispatcher?: Dispatcher } = {
      headers: {
        Accept: 'application/json',
        'User-Agent': USER_AGENT,
      },
      signal: abort.signal,
    }
    if (dispatcher) init.dispatcher = dispatcher
    res = await fetch(url, init as RequestInit)
  } catch (err) {
    return { kind: 'TRANSIENT', message: String(err) }
  } finally {
    clearTimeout(timer)
  }

  if (res.status === 404)
    return { kind: 'NOT_FOUND', message: `${name} not found`, statusCode: 404 }
  if (res.status === 429) return { kind: 'RATE_LIMIT', message: 'rate limited', statusCode: 429 }
  if (!res.ok) return { kind: 'TRANSIENT', message: `HTTP ${res.status}`, statusCode: res.status }

  let json: unknown
  try {
    json = await res.json()
  } catch {
    return { kind: 'MALFORMED', message: 'invalid JSON' }
  }

  if (!isPackument(json)) return { kind: 'MALFORMED', message: 'unexpected shape' }
  delete (json as unknown as Record<string, unknown>).readme
  return json
}

function isPackument(v: unknown): v is Packument {
  return typeof v === 'object' && v !== null && 'name' in v && 'versions' in v && 'dist-tags' in v
}
