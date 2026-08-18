import type { Dispatcher } from 'undici'

import { combineSignals } from './signals'
import { type FetchError, FetchErrorKind, type Packument } from './types'

const REGISTRY = 'https://registry.npmjs.org'
const USER_AGENT = 'lfx-packages-worker/0.1 (+https://lfx.linuxfoundation.org)'

function encodeNpmName(name: string): string {
  return name.startsWith('@') ? `@${encodeURIComponent(name.slice(1))}` : encodeURIComponent(name)
}

// `dispatcher` (an undici ProxyAgent) routes the request through a specific proxy IP
// so concurrent ingest lanes each use their own egress address / rate limit.
// `signal` (e.g. a Temporal activity's cancellationSignal) is combined with the
// request's own 30s timeout so a caller can abort in-flight requests early.
export async function fetchPackument(
  name: string,
  dispatcher?: Dispatcher,
  signal?: AbortSignal,
): Promise<Packument | FetchError> {
  const url = `${REGISTRY}/${encodeNpmName(name)}`
  const abort = new AbortController()
  const timer = setTimeout(() => abort.abort(), 30_000)
  const combinedSignal = combineSignals(abort.signal, signal)
  let res: Response
  try {
    // Node 24 and undici 5 have different Dispatcher types.
    res = await fetch(url, {
      headers: {
        Accept: 'application/json',
        'User-Agent': USER_AGENT,
      },
      signal: combinedSignal,
      dispatcher,
    } as RequestInit)
  } catch (err) {
    return { kind: FetchErrorKind.TRANSIENT, message: String(err) }
  } finally {
    clearTimeout(timer)
  }

  if (res.status === 404)
    return { kind: FetchErrorKind.NOT_FOUND, message: `${name} not found`, statusCode: 404 }
  if (res.status === 429) {
    return {
      kind: FetchErrorKind.RATE_LIMIT,
      message: 'rate limited',
      statusCode: 429,
      retryAfterSec: parseRetryAfterSec(res.headers.get('retry-after')),
    }
  }
  if (!res.ok)
    return { kind: FetchErrorKind.TRANSIENT, message: `HTTP ${res.status}`, statusCode: res.status }

  let json: unknown
  try {
    json = await res.json()
  } catch {
    return { kind: FetchErrorKind.MALFORMED, message: 'invalid JSON' }
  }

  if (!isPackument(json)) {
    const stub = asUnpublishedStub(json)
    if (stub) return stub
    return { kind: FetchErrorKind.MALFORMED, message: 'unexpected shape' }
  }
  delete (json as unknown as Record<string, unknown>).readme
  return json
}

// Retry-After is either delta-seconds or an HTTP-date (RFC 9110). Returns whole seconds,
// or undefined when absent/unparseable/expired so the caller applies its own default.
function parseRetryAfterSec(header: string | null): number | undefined {
  if (!header) return undefined
  const seconds = Number(header)
  if (Number.isFinite(seconds)) return seconds > 0 ? Math.ceil(seconds) : undefined
  const date = new Date(header)
  if (Number.isNaN(date.getTime())) return undefined
  const untilSec = Math.ceil((date.getTime() - Date.now()) / 1000)
  return untilSec > 0 ? untilSec : undefined
}

function isPackument(v: unknown): v is Packument {
  return typeof v === 'object' && v !== null && 'name' in v && 'versions' in v && 'dist-tags' in v
}

// A fully unpublished package returns HTTP 200 with a stub document — just name + time,
// where time.unpublished records the unpublish event; there are no versions/dist-tags keys,
// so isPackument rejects it. Normalize the stub into an empty packument with `unpublished`
// set, so ingest stores status='unpublished' instead of erroring on shape.
function asUnpublishedStub(v: unknown): Packument | null {
  if (typeof v !== 'object' || v === null) return null
  const o = v as Record<string, unknown>
  if (typeof o.name !== 'string' || typeof o.time !== 'object' || o.time === null) return null
  const t = o.time as Record<string, unknown>
  const unpublished = t.unpublished
  if (typeof unpublished !== 'object' || unpublished === null) return null
  if (typeof (unpublished as Record<string, unknown>).time !== 'string') return null
  const time: Record<string, string> = {}
  for (const [key, value] of Object.entries(t)) {
    if (typeof value === 'string') time[key] = value
  }
  return { name: o.name, 'dist-tags': {}, versions: {}, time, unpublished }
}
