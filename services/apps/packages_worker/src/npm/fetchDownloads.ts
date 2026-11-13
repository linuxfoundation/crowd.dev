import type { Dispatcher } from 'undici'

import { BULK_MAX_NAMES } from './downloadGaps'
import { combineSignals } from './signals'
import { type FetchError, FetchErrorKind, isFetchError } from './types'

const USER_AGENT = 'lfx-packages-worker/0.1 (+https://lfx.linuxfoundation.org)'

function encodeNpmName(name: string): string {
  return name.startsWith('@') ? `@${encodeURIComponent(name.slice(1))}` : encodeURIComponent(name)
}

function downloadInit(dispatcher?: Dispatcher, signal?: AbortSignal): RequestInit {
  return {
    headers: { 'User-Agent': USER_AGENT },
    signal,
    dispatcher,
  }
}

export interface PointRangeResult {
  count: number
  start: string
  end: string
}

export interface BulkPointRangeResult {
  counts: Map<string, number> // name → download count (absent = not found / no data)
  start: string
  end: string
}

// GET api.npmjs.org/downloads/point/<start>:<end>/<pkg1>,<pkg2>,...
// Supports up to 128 unscoped packages per request. Scoped packages are not supported.
// Packages with no data are returned as null in the response and are absent from counts.
export async function fetchBulkPointRange(
  names: string[],
  start: string,
  end: string,
  dispatcher?: Dispatcher,
  signal?: AbortSignal,
): Promise<BulkPointRangeResult | FetchError> {
  if (names.length > 128)
    throw new Error(`fetchBulkPointRange: too many names (${names.length} > 128)`)
  const url = `https://api.npmjs.org/downloads/point/${start}:${end}/${names.join(',')}`
  const abort = new AbortController()
  const timer = setTimeout(() => abort.abort(), 30_000)
  let res: Response
  try {
    res = await fetch(url, downloadInit(dispatcher, combineSignals(abort.signal, signal)))
  } catch (err) {
    return { kind: FetchErrorKind.TRANSIENT, message: String(err) }
  } finally {
    clearTimeout(timer)
  }

  if (res.status === 404)
    return { kind: FetchErrorKind.NOT_FOUND, message: `bulk request not found`, statusCode: 404 }
  if (res.status === 429) {
    const headers: Record<string, string> = {}
    for (const [k, v] of res.headers.entries()) headers[k] = v
    const body = await res.text().catch(() => '')
    return {
      kind: FetchErrorKind.RATE_LIMIT,
      message: `rate limited by npm downloads API — headers: ${JSON.stringify(headers)} body: ${body}`,
      statusCode: 429,
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

  if (typeof json !== 'object' || json === null)
    return { kind: FetchErrorKind.MALFORMED, message: 'expected object response' }

  const raw = json as Record<string, { downloads?: unknown; start?: unknown; end?: unknown } | null>
  const counts = new Map<string, number>()
  let respStart = start
  let respEnd = end

  for (const [name, entry] of Object.entries(raw)) {
    if (entry === null || typeof entry.downloads !== 'number') continue
    counts.set(name, entry.downloads)
    if (typeof entry.start === 'string') respStart = entry.start
    if (typeof entry.end === 'string') respEnd = entry.end
  }

  return { counts, start: respStart, end: respEnd }
}

// GET api.npmjs.org/downloads/point/<start>:<end>/<name> — returns total downloads for the range.
export async function fetchPointRange(
  name: string,
  start: string,
  end: string,
  dispatcher?: Dispatcher,
  signal?: AbortSignal,
): Promise<PointRangeResult | FetchError> {
  const url = `https://api.npmjs.org/downloads/point/${start}:${end}/${encodeNpmName(name)}`
  const abort = new AbortController()
  const timer = setTimeout(() => abort.abort(), 30_000)
  let res: Response
  try {
    res = await fetch(url, downloadInit(dispatcher, combineSignals(abort.signal, signal)))
  } catch (err) {
    return { kind: FetchErrorKind.TRANSIENT, message: String(err) }
  } finally {
    clearTimeout(timer)
  }

  if (res.status === 404)
    return { kind: FetchErrorKind.NOT_FOUND, message: `${name} not found`, statusCode: 404 }
  if (res.status === 429) {
    const headers: Record<string, string> = {}
    for (const [k, v] of res.headers.entries()) headers[k] = v
    const body = await res.text().catch(() => '')
    return {
      kind: FetchErrorKind.RATE_LIMIT,
      message: `rate limited by npm downloads API — headers: ${JSON.stringify(headers)} body: ${body}`,
      statusCode: 429,
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

  const data = json as { downloads?: unknown; start?: unknown; end?: unknown }
  if (typeof data.downloads !== 'number')
    return { kind: FetchErrorKind.MALFORMED, message: 'missing downloads field' }

  return {
    count: data.downloads,
    start: typeof data.start === 'string' ? data.start : start,
    end: typeof data.end === 'string' ? data.end : end,
  }
}

export interface DailyDownloadsResult {
  start: string
  end: string
  downloads: Array<{ day: string; downloads: number }>
}

// GET api.npmjs.org/downloads/range/<start>:<end>/<name> — max 18 months per call.
export async function fetchDailyRange(
  name: string,
  start: string,
  end: string,
  dispatcher?: Dispatcher,
): Promise<DailyDownloadsResult | FetchError> {
  const url = `https://api.npmjs.org/downloads/range/${start}:${end}/${encodeNpmName(name)}`
  const abort = new AbortController()
  const timer = setTimeout(() => abort.abort(), 30_000)
  let res: Response
  try {
    res = await fetch(url, downloadInit(dispatcher, abort.signal))
  } catch (err) {
    return { kind: FetchErrorKind.TRANSIENT, message: String(err) }
  } finally {
    clearTimeout(timer)
  }

  if (res.status === 404)
    return { kind: FetchErrorKind.NOT_FOUND, message: `${name} not found`, statusCode: 404 }
  if (res.status === 429) {
    const headers: Record<string, string> = {}
    for (const [k, v] of res.headers.entries()) headers[k] = v
    const body = await res.text().catch(() => '')
    return {
      kind: FetchErrorKind.RATE_LIMIT,
      message: `rate limited by npm downloads API — headers: ${JSON.stringify(headers)} body: ${body}`,
      statusCode: 429,
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

  const data = json as { start?: unknown; end?: unknown; downloads?: unknown }
  if (!Array.isArray(data.downloads))
    return { kind: FetchErrorKind.MALFORMED, message: 'missing downloads array' }

  return {
    start: typeof data.start === 'string' ? data.start : start,
    end: typeof data.end === 'string' ? data.end : end,
    downloads: (data.downloads as Array<{ day: unknown; downloads: unknown }>)
      .filter((d) => typeof d.day === 'string' && typeof d.downloads === 'number')
      .map((d) => ({ day: d.day as string, downloads: d.downloads as number })),
  }
}

// Shared request and status mapping for the downloads API; resolves to the parsed body.
async function getDownloadsJson(
  url: string,
  notFoundMessage: string,
  dispatcher?: Dispatcher,
  signal?: AbortSignal,
): Promise<{ json: unknown } | FetchError> {
  const abort = new AbortController()
  const timer = setTimeout(() => abort.abort(), 30_000)
  try {
    let res: Response
    try {
      res = await fetch(url, downloadInit(dispatcher, combineSignals(abort.signal, signal)))
    } catch (err) {
      return { kind: FetchErrorKind.TRANSIENT, message: String(err) }
    }

    if (res.status === 404)
      return { kind: FetchErrorKind.NOT_FOUND, message: notFoundMessage, statusCode: 404 }
    if (res.status === 429) {
      const headers: Record<string, string> = {}
      for (const [k, v] of res.headers.entries()) headers[k] = v
      const body = await res.text().catch(() => '')
      return {
        kind: FetchErrorKind.RATE_LIMIT,
        message: `rate limited by npm downloads API, headers: ${JSON.stringify(headers)} body: ${body}`,
        statusCode: 429,
      }
    }
    if (!res.ok)
      return {
        kind: FetchErrorKind.TRANSIENT,
        message: `HTTP ${res.status}`,
        statusCode: res.status,
      }

    let text: string
    try {
      text = await res.text()
    } catch (err) {
      return { kind: FetchErrorKind.TRANSIENT, message: `body read failed: ${String(err)}` }
    }
    try {
      return { json: JSON.parse(text) }
    } catch {
      return { kind: FetchErrorKind.MALFORMED, message: 'invalid JSON' }
    }
  } finally {
    clearTimeout(timer)
  }
}

// GET api.npmjs.org/downloads/point/last-day: the newest day npm has published counts for.
export async function fetchLastPublishedDay(
  dispatcher?: Dispatcher,
  signal?: AbortSignal,
): Promise<{ day: string } | FetchError> {
  const res = await getDownloadsJson(
    'https://api.npmjs.org/downloads/point/last-day',
    'last-day not found',
    dispatcher,
    signal,
  )
  if (isFetchError(res)) return res
  const end = (res.json as { end?: unknown } | null)?.end
  if (typeof end !== 'string') return { kind: FetchErrorKind.MALFORMED, message: 'missing end day' }
  return { day: end }
}

export interface BulkDailyRangeResult {
  downloads: Map<string, Array<{ day: string; downloads: number }>> // absent = not found
}

// GET api.npmjs.org/downloads/range/<start>:<end>/<pkg1>,<pkg2>,... for 2 to 128 unscoped names.
// npm answers a single name in the single-package shape, so one name is rejected.
export async function fetchBulkDailyRange(
  names: string[],
  start: string,
  end: string,
  dispatcher?: Dispatcher,
  signal?: AbortSignal,
): Promise<BulkDailyRangeResult | FetchError> {
  if (names.length < 2 || names.length > BULK_MAX_NAMES)
    throw new Error(
      `fetchBulkDailyRange: expected 2 to ${BULK_MAX_NAMES} names, got ${names.length}`,
    )
  const res = await getDownloadsJson(
    `https://api.npmjs.org/downloads/range/${start}:${end}/${names.join(',')}`,
    'bulk request not found',
    dispatcher,
    signal,
  )
  if (isFetchError(res)) return res
  if (typeof res.json !== 'object' || res.json === null || Array.isArray(res.json))
    return { kind: FetchErrorKind.MALFORMED, message: 'expected object response' }

  const downloads = new Map<string, Array<{ day: string; downloads: number }>>()
  for (const [name, entry] of Object.entries(res.json as Record<string, unknown>)) {
    if (entry === null) continue
    const rows =
      typeof entry === 'object' ? (entry as { downloads?: unknown }).downloads : undefined
    if (!Array.isArray(rows))
      return { kind: FetchErrorKind.MALFORMED, message: `unexpected bulk entry for ${name}` }
    downloads.set(
      name,
      (rows as Array<{ day: unknown; downloads: unknown }>)
        .filter((d) => typeof d.day === 'string' && typeof d.downloads === 'number')
        .map((d) => ({ day: d.day as string, downloads: d.downloads as number })),
    )
  }
  return { downloads }
}
