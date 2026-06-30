import type { Dispatcher } from 'undici'

import type { FetchError, PyPiProject } from './types'

const REGISTRY = 'https://pypi.org/pypi'
const USER_AGENT = 'lfx-packages-worker/0.1 (+https://lfx.linuxfoundation.org)'

// Fetch a project's metadata from the PyPI JSON API.
// Error's handled with respect to their types (retryable or not)
// 404 → NOT_FOUND (skip)
// 429 → RATE_LIMIT and 5xx/network → TRANSIENT (Temporal retries)
// malformed body → MALFORMED (skip).
// `dispatcher` routes the request through a proxy IP when the
// optional PyPI proxy layer is enabled; undefined means direct egress.
export async function fetchProject(
  name: string,
  dispatcher?: Dispatcher,
): Promise<PyPiProject | FetchError> {
  const url = `${REGISTRY}/${encodeURIComponent(name)}/json`
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

  if (!isPyPiProject(json)) return { kind: 'MALFORMED', message: 'unexpected shape' }
  return json
}

function isPyPiProject(v: unknown): v is PyPiProject {
  if (typeof v !== 'object' || v === null || !('info' in v)) return false
  const info = (v as { info: unknown }).info
  return (
    typeof info === 'object' &&
    info !== null &&
    typeof (info as { name?: unknown }).name === 'string'
  )
}
